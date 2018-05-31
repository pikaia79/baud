package null

import (
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
)

var _ kvstore.KVStore = &Store{}
var _ kvstore.KVIterator = &iterator{}
var _ kvstore.KVBatch = &batch{}

type Store struct{}

func New() (kvstore.KVStore, error) {
	return &Store{}, nil
}

func (r *Store) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (r *Store) Delete(key []byte) error {
	return nil
}

func (r *Store) Put(key, value []byte) error {
	return nil
}

func (r *Store) MultiGet(keys [][]byte) ([][]byte, error) {
	return make([][]byte, len(keys)), nil
}

func (r *Store) PrefixIterator(prefix []byte) kvstore.KVIterator {
	return &iterator{}
}

func (r *Store) RangeIterator(start, end []byte) kvstore.KVIterator {
	return &iterator{}
}

func (w *Store) NewKVBatch() kvstore.KVBatch {
	return &batch{}
}

func (w *Store) ExecuteBatch(batch kvstore.KVBatch) error {
	return nil
}

func (w *Store) NewTransaction(writable bool) (kvstore.Transaction, error) {
	return &transaction{}, nil
}

func (r *Store) Close() error {
	return nil
}

type iterator struct{}

func (i *iterator) SeekFirst()    {}
func (i *iterator) Seek(k []byte) {}
func (i *iterator) Next()         {}

func (i *iterator) Current() ([]byte, []byte, bool) {
	return nil, nil, false
}

func (i *iterator) Key() []byte {
	return nil
}

func (i *iterator) Value() []byte {
	return nil
}

func (i *iterator) Valid() bool {
	return false
}

func (i *iterator) Close() error {
	return nil
}

func (i *Store) GetSnapshot() (kvstore.Snapshot, error) {
	return &reader{}, nil
}

type reader struct{}

func (r *reader) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (r *reader) MultiGet(keys [][]byte) ([][]byte, error) {
	return make([][]byte, len(keys)), nil
}

func (r *reader) PrefixIterator(prefix []byte) kvstore.KVIterator {
	return &iterator{}
}

func (r *reader) RangeIterator(start, end []byte) kvstore.KVIterator {
	return &iterator{}
}

func (r *reader) Close() error {
	return nil
}

type batch struct{}

func (i *batch) Set(key, val []byte)             {}
func (i *batch) Delete(key []byte)               {}
func (i *batch) Merge(key, val []byte)           {}
func (i *batch) Reset()                          {}
func (i *batch) Operations() []kvstore.Operation { return nil }
func (i *batch) Close() error                    { return nil }

type transaction struct {}

func(tx *transaction) Put(key, value []byte) error {return nil}
func(tx *transaction) Get(key []byte) ([]byte, error) {return nil, nil}
func(tx *transaction) Delete(key []byte) error {return nil}
func(tx *transaction) PrefixIterator(prefix []byte) kvstore.KVIterator {return &iterator{}}
func(tx *transaction) RangeIterator(start, end []byte) kvstore.KVIterator {return &iterator{}}
func (tx *transaction) Commit() error { return nil }
func (tx *transaction) Rollback() error { return nil }
