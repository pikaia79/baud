package store

import (
	"github.com/tiglabs/baud/document"
	"github.com/tiglabs/baud/kernel/store/kvstore"
	"github.com/tiglabs/baud/kernel/mapping"
)

type DocStore struct {
	store       kvstore.KVStore
	mapper      *mapping.Mapper
}

func NewDocStore(store kvstore.KVStore) *DocStore {
	return &DocStore{store: store}
}

func(s *DocStore) InsertDoc(doc *document.Document) error {
	// now we only store source
	key := encodeStoreKey(doc.SlotID, doc.ID)
	value := encodeStoreValue(doc.Source)

	return s.store.Put(key, value)
}

func(s *DocStore) QueryDoc(slotID uint32, id string) (*document.Document, error) {
	key := encodeStoreKey(slotID, id)
	value, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}
	document.NewDocument(id, slotID, 0, value)
	return nil, nil
}

func(s *DocStore) MultiQueryDoc(slotID uint32, ids []string) ([]*document.Document, error) {
	var keys [][]byte
	for _, id := range ids {
		keys = append(keys, encodeStoreKey(slotID, id))
	}
	values, err := s.store.MultiGet(keys)
	if err != nil {
		return nil, err
	}
	var docs []*document.Document
	for i, id := range ids {
		docs = append(docs, document.NewDocument(id, slotID, 0, values[i]))
	}
	return docs, nil
}

func(s *DocStore) DeleteDoc(slotID uint32, id string) error {
	key := encodeStoreKey(slotID, id)
	return s.store.Delete(key)
}

func(s *DocStore) Close()
