package index

import (
	"bytes"
	"encoding/binary"
	"errors"

	"golang.org/x/net/context"

	"github.com/tiglabs/baudengine/kernel"
	"github.com/tiglabs/baudengine/kernel/document"
	"github.com/tiglabs/baudengine/kernel/mapping"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
	"github.com/tiglabs/baudengine/kernel/util"
)

var RAFT_APPLY_ID []byte = []byte("Raft_apply_id")

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

func (id *IndexDriver) addDocument(tx kvstore.Transaction, doc *document.Document) error {
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
		tx.Put(key, row)
	}
	return nil
}

func (id *IndexDriver) AddDocument(ctx context.Context, doc *document.Document, applyID uint64) error {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		id.SetApplyID(applyID)
		return err
	}
	err = id.addDocument(tx, doc)
	if err != nil {
		tx.Rollback()
		id.SetApplyID(applyID)
		return err
	}
	if applyID > 0 {
		var buff [8]byte
		binary.BigEndian.PutUint64(buff[:], applyID)
		tx.Put(RAFT_APPLY_ID, buff[:])
	}
	return tx.Commit()
}

// parameter docs must merge before update
// IndexDriver UpdateDocuments API will be called by ps when raft apply
func (id *IndexDriver) UpdateDocument(ctx context.Context, doc *document.Document, upsert bool, applyID uint64) (found bool, err error) {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		id.SetApplyID(applyID)
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
		_, err = id.deleteDocument(tx, doc.ID)
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
			tx.Put(key, row)
		}
		// todo analysis document
		return true, nil
	}()
	if err != nil {
		tx.Rollback()
		id.SetApplyID(applyID)
		return found, err
	}
	if !found && upsert {
		err = id.addDocument(tx, doc)
		if err != nil {
			tx.Rollback()
			id.SetApplyID(applyID)
			return found, err
		}
	}
	if applyID > 0 {
		var buff [8]byte
		binary.BigEndian.PutUint64(buff[:], applyID)
		tx.Put(RAFT_APPLY_ID, buff[:])
	}
	return found, tx.Commit()
}

func (id *IndexDriver) DeleteDocument(ctx context.Context, docID []byte, applyID uint64) (int, error) {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		id.SetApplyID(applyID)
		return 0, err
	}
	var count int
	n, err := id.deleteDocument(tx, docID)
	if err != nil {
		tx.Rollback()
		id.SetApplyID(applyID)
		return count, err
	}
	if n > 0 {
		count++
	}
	if applyID > 0 {
		var buff [8]byte
		binary.BigEndian.PutUint64(buff[:], applyID)
		tx.Put(RAFT_APPLY_ID, buff[:])
	}
	err = tx.Commit()
	return count, err
}

// source set true means need return _source
func (id *IndexDriver) GetDocument(ctx context.Context, docID []byte, fields []string) (map[string]interface{}, bool) {
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
			for _, f := range fs {
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

func (id *IndexDriver) SetApplyID(applyID uint64) error {
	if applyID > 0 {
		var buff [8]byte
		binary.BigEndian.PutUint64(buff[:], applyID)
		return id.store.Put(RAFT_APPLY_ID, buff[:])
	}
	return nil
}

func (id *IndexDriver) GetDocSnapshot() (kernel.Snapshot, error) {
	snap, err := id.store.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &DocSnapshot{snap: snap}, nil
}

// TODO clear store kv paris before apply snapshot
func (id *IndexDriver) ApplyDocSnapshot(ctx context.Context, iter kernel.Iterator) error {
	var batch kvstore.KVBatch
	count := 0
	for iter.Valid() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if batch == nil {
			batch = id.store.NewKVBatch()
		}
		batch.Set(iter.Key(), iter.Value())
		count++
		if count%100 == 0 {
			err := id.store.ExecuteBatch(batch)
			if err != nil {
				return err
			}
			batch = nil
		}
		iter.Next()
	}
	if batch != nil {
		return id.store.ExecuteBatch(batch)
	}
	return nil
}

func (id *IndexDriver) GetApplyID() (uint64, error) {
	if id == nil {
		return 0, nil
	}
	v, err := id.store.Get(RAFT_APPLY_ID)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, errors.New("invalid applyID value in store")
	}
	return binary.BigEndian.Uint64(v), nil
}

func (id *IndexDriver) Close() error {
	return id.store.Close()
}

func (id *IndexDriver) deleteDocument(tx kvstore.Transaction, docID []byte) (int, error) {
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
		err := tx.Delete(key)
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
