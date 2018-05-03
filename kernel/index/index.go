package index

import (
	"bytes"
	"errors"

	"github.com/tiglabs/baudengine/kernel/document"
	"github.com/tiglabs/baudengine/kernel/mapping"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
	"github.com/tiglabs/baudengine/kernel/util"
	"github.com/tiglabs/baudengine/kernel"
)

var _ kernel.Engine = &IndexDriver{}

type IndexDriver struct {
	store        kvstore.KVStore
	indexMapping mapping.IndexMapping
}

func NewIndexDriver(store kvstore.KVStore) *IndexDriver {
	return &IndexDriver{
		store: store,
	}
}

func (id *IndexDriver) addDocument(tx kvstore.Transaction, doc *document.Document, applyId uint64) error {
	val, err := tx.Get(encodeStoreFieldKey(doc.ID, "_version"))
	if err != nil {
		return err
	}
	if val != nil {
		return errors.New("document exist")
	}
	for _, fields := range doc.Fields {
		key, row, err := encodeStoreField(doc.ID, fields)
		if err != nil {
			return err
		}
		tx.Put(key, row, &kvstore.Option{ApplyID: applyId})
	}
	return nil
}

func (id *IndexDriver) AddDocument(doc *document.Document, applyId uint64) error {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		return err
	}
	err = id.addDocument(tx, doc, applyId)
	if err != nil {
		return tx.Rollback()
	}
	return tx.Commit()
}

// parameter docs must merge before update
// IndexDriver UpdateDocuments API will be called by ps when raft apply
func (id *IndexDriver) UpdateDocument(doc *document.Document, upsert bool, applyId uint64) (found bool, err error) {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		return false, err
	}
	found, err = func() (bool, error) {
		// get old document
		iter := tx.PrefixIterator(encodeStoreFieldKey(doc.ID, ""))
		oldDoc, err, ok := getDocument(doc.ID, iter)
		iter.Close()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}

		// check version
		fs := oldDoc.FindFields("_version")
		if len(fs) == 0 {
			return false, errors.New("invalid document, has no _version field")
		}
		oldVersion := fs[0].Value()
		fs = doc.FindFields("_version")
		if len(fs) == 0 {
			return false, errors.New("invalid document, has no _version field")
		}
		srcVersion := fs[0].Value()
		if bytes.Compare(oldVersion, srcVersion) != 0 {
			return true, errors.New("version conflict")
		}
		_, err = id.deleteDocument(tx, doc.ID, applyId)
		if err != nil {
			return true, err
		}
		// todo delete old document(send delete event)
		err = id.deleteDocumentIndex(oldDoc)
		if err != nil {
			return true, err
		}
		// fixme _source, _all, _version need special handle
		for _, fields := range doc.Fields {
			key, row, err := encodeStoreField(doc.ID, fields)
			if err != nil {
				return true, err
			}
			tx.Put(key, row, &kvstore.Option{ApplyID: applyId})
		}
		// todo analysis document
		return true, nil
	}()
	if err != nil {
		tx.Rollback()
		return found, err
	}
	if !found && upsert {
		err = id.addDocument(tx, doc, applyId)
		if err != nil {
			tx.Rollback()
			return found, err
		}
	}
	return found, tx.Commit()
}

func (id *IndexDriver) DeleteDocument(docID []byte, applyId uint64) (int, error) {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		return 0, err
	}
	var count int
	n, err := id.deleteDocument(tx, docID, applyId)
	if err != nil {
		tx.Rollback()
		return count, err
	}
	count += n
	err = tx.Commit()

	return count, err
}

// source set true means need return _source
func (id *IndexDriver) GetDocument(docID []byte, fields []string) (map[string]interface{}, bool) {
	if len(fields) == 0 {
		return nil, false
	}
	var keys [][]byte
	for _, fieldName := range fields {
		keys = append(keys, encodeStoreFieldKey(docID, fieldName))
	}
	values, err := id.store.MultiGet(keys)
	if err != nil {
		return nil, false
	}
	doc := document.NewDocument(docID)
	for i, fieldName := range fields {
		fs, err := decodeStoreField(fieldName, values[i])
		if err != nil {
			return nil, false
		}
		for _, f := range fs {
			doc.AddField(f)
		}
	}
	fieldValues := make(map[string]interface{})
	for name, fs := range doc.Fields {
		if len(fs) == 1 {
			v, err := getFieldValue(fs[0])
			if err != nil {
				return nil, false
			}
			fieldValues[name] = v
		} else {
			var vs []interface{}
			for _, f :=  range fs {
				v, err := getFieldValue(f)
				if err != nil {
					return nil, false
				}
				vs = append(vs, v)
			}
			fieldValues[name] = vs
		}
	}
	return fieldValues, true
}

func (id *IndexDriver) GetLastApplyID() (uint64, error) {
	if id == nil {
		return 0, nil
	}
	snap, err := id.store.GetSnapshot()
	if err != nil {
		return 0, err
	}
	op, err := snap.LastOption()
	if err != nil {
		return 0, err
	}
	return op.ApplyID, nil
}

func (id *IndexDriver) Close() error {
	return id.store.Close()
}

func (id *IndexDriver) deleteDocument(tx kvstore.Transaction, docID []byte, applyId uint64) (int, error) {
	iter := tx.PrefixIterator(encodeStoreFieldKey(docID, ""))
	defer iter.Close()
	if !iter.Valid() {
		return 0, nil
	}
	count := 0
	for iter.Valid() {
		key, _, has := iter.Current()
		if !has {
			return 0, errors.New("document not exist")
		}
		err := tx.Delete(key, &kvstore.Option{ApplyID: applyId})
		if err != nil {
			return 0, err
		}
		count++
		iter.Next()
	}
	return count, nil
}

// TODO
func (id *IndexDriver) deleteDocumentIndex(doc *document.Document) error {
	return nil
}

func getDocument(docID []byte, iter kvstore.KVIterator) (*document.Document, error, bool) {
	doc := document.NewDocument(docID)
	for iter.Valid() {
		key, value, has := iter.Current()
		if !has {
			return nil, nil, false
		}
		fileName, err := decodeStoreFieldKey(key)
		if err != nil {
			return nil, err, false
		}
		fields, err := decodeStoreField(fileName, util.CloneBytes(value))
		if err != nil {
			return nil, err, false
		}
		for _, field := range fields {
			doc.AddField(field)
		}
		iter.Next()
	}
	return doc, nil, true
}

func getFieldValue(field document.Field) (interface{}, error) {
	switch f := field.(type) {
	case *document.TextField:
		return string(f.Value()), nil
	case *document.BooleanField:
		return f.Boolean()
	case *document.DateTimeField:
		return f.DateTime()
	case *document.NumericField:
		return f.Number()
	default:
		return nil, errors.New("invalid field type")
	}
}
