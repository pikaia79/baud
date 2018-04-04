package store

import (
	// add github.com/tiglabs/ before baud
	"baud/kernel/document"
	"baud/kernel/store/kvstore"
)

type DocStore struct {
	store       kvstore.KVStore
}

func NewDocStore(store kvstore.KVStore) *DocStore {
	return &DocStore{store: store}
}

func(s *DocStore) InsertEntity(doc *document.Entity) error {
	// now we only store source
	key, err := encodeStoreKey(doc.UID())
	if err != nil {
		return err
	}
	d, err := doc.Document()
	if err != nil {
		return err
	}
	value, _ := encodeStoreValue(d.Doc())

	return s.store.Put(key, value)
}

func(s *DocStore) QueryEntity(uid document.UID) (*document.Entity, error) {
	// now we only store source
	key, err := encodeStoreKey(uid)
	if err != nil {
		return nil, err
	}
	value, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}
	return document.NewEntity(uid, value), nil
}

func(s *DocStore) DeleteEntity(uid document.UID) error {
	key, err := encodeStoreKey(uid)
	if err != nil {
		return err
	}
	return s.store.Delete(key)
}

func(s *DocStore) InsertEdge(edge *document.Edge) error {
	// now we only store source
	src, dst, err := edge.Nodes()
	if err != nil {
		return err
	}
	// must <src, dst> for store key!!!!
	key, err := encodeStoreKey(src, dst)
	if err != nil {
		return err
	}
	d, err := edge.Document()
	if err != nil {
		return err
	}
	value, _ := encodeStoreValue(d.Doc())
	return s.store.Put(key, value)
}

func(s *DocStore) QueryEdge(src, dst document.UID) (*document.Edge, error) {
	// must <src, dst> for store key!!!!
	key, err := encodeStoreKey(src, dst)
	if err != nil {
		return nil, err
	}
	value, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}
	return document.NewEdge(src, dst, value), nil
}

func(s *DocStore) DeleteEdge(src, dst document.UID) error {
	// must <src, dst> for store key!!!!
	key, err := encodeStoreKey(src, dst)
	if err != nil {
		return err
	}
	return s.store.Delete(key)
}

func(s *DocStore) Close()
