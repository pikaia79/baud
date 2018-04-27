package index

import (
	"errors"
	"bytes"

	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/store/kvstore"
	"github.com/tiglabs/baud/kernel/util"
	"github.com/tiglabs/baud/kernel/mapping"
	"github.com/tiglabs/baud/kernel"
)

type IndexDriver struct {
	store        kvstore.KVStore
	indexMapping mapping.IndexMapping
}

func (id *IndexDriver) AddDocuments(docs []*document.Document, ops ...*kernel.Option) error {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		return err
	}
	err = func () error {
		for _, doc := range docs {
			// check doc version
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
		}
		return nil
	}()
	if err != nil {
		return tx.Rollback()
	}
	return tx.Commit()
}

// parameter docs must merge before update
// IndexDriver UpdateDocuments API will be called by ps when raft apply
func (id *IndexDriver) UpdateDocuments(docs []*document.Document, ops ...*kernel.Option) error {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		return err
	}
	err = func () error {
		for _, doc := range docs {
			// get old document
			iter := tx.PrefixIterator(encodeStoreFieldKey(doc.ID, ""))
			oldDoc, err := getDocument(doc.ID, iter)
			iter.Close()
			if err != nil {
				return err
			}
			// check version
			fs := oldDoc.FindFields("_version")
			oldVersion := fs[0].Value()
			fs = doc.FindFields("_version")
			srcVersion := fs[0].Value()
			if bytes.Compare(oldVersion, srcVersion) != 0 {
				return errors.New("version conflict")
			}
			_, err = id.deleteDocument(tx, doc.ID)
			if err != nil {
				return err
			}
			// todo delete old document(send delete event)
			err = id.deleteDocumentIndex(oldDoc)
			if err != nil {
				return err
			}
			// fixme _source, _all, _version need special handle
			for _, fields := range doc.Fields {
				key, row, err := encodeStoreField(doc.ID, fields)
				if err != nil {
					return err
				}
				tx.Put(key, row)
			}
			// todo analysis document
		}
		return nil
	}()
	if err != nil {
		return tx.Rollback()
	}
	return tx.Commit()
}

func (id *IndexDriver) DeleteDocuments(docIDs []string, ops ...*kernel.Option) (int, error) {
	tx, err := id.store.NewTransaction(true)
	if err != nil {
		return 0, err
	}
	var count int
	for _, docID := range docIDs {
		n, err := id.deleteDocument(tx, docID)
		if err != nil {
			tx.Rollback()
			return count, err
		}
		count += n
	}
	err = tx.Commit()
	return count, err
}

// source set true means need return _source
func (id *IndexDriver) GetDocument(docID string, fields []string) (*document.Document, bool) {
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
	return doc, true
}

func (id *IndexDriver) deleteDocument(tx kvstore.Transaction, docID string) (int, error) {
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

func getDocument(docID string, iter kvstore.KVIterator) (*document.Document, error) {
	doc := document.NewDocument(docID)
	for iter.Valid() {
		key, value, has := iter.Current()
		if !has {
			return nil, errors.New("document not exist")
		}
		fileName, err := decodeStoreFieldKey(key)
		if err != nil {
			return nil, err
		}
		fields, err := decodeStoreField(fileName, util.CloneBytes(value))
		if err != nil {
			return nil, err
		}
		for _, field := range fields {
			doc.AddField(field)
		}
		iter.Next()
	}
	return doc, nil
}
