package null

import (
	"github.com/tiglabs/baud/kernel/store/memstore"
)

var _ memstore.MemStore = &Store{}
var _ memstore.MemBatch = &batch{}

type Store struct{}

func New() (memstore.MemStore, error) {
	return &Store{}, nil
}

// value is an object for memory, and it can be changed out of the store
<<<<<<< HEAD
func (r *Store)Put(key []byte, value interface{}) error {
	return nil
}

func (r *Store)Get(key []byte) (interface{}, error) {
	return nil, nil
}

func (r *Store)Delete(key []byte) error {
	return nil
}
=======
func (r *Store) Put(key []byte, value interface{}) error
func (r *Store) Get(key []byte) (interface{}, error)
func (r *Store) Delete(key []byte) error
>>>>>>> upstream/master

// PrefixIterator will visit all K/V pairs with the provided prefix.
// Returns an error if the store is closed or the tx is closed.
// Because it will block the write operation, users should
// not do slow operation as much as possible.
<<<<<<< HEAD
func (r *Store)PrefixIterator(prefix []byte, iter memstore.IterFunc) error {
	return nil
}
=======
func (r *Store) PrefixIterator(prefix []byte, iter IterFunc) error
>>>>>>> upstream/master

// RangeIterator will visit all K/V pairs >= start AND < end
// Returns an error if the store is closed or the tx is closed.
// Because it will block the write operation, users should
// not do slow operation as much as possible.
<<<<<<< HEAD
func (r *Store)RangeIterator(start, end []byte, iter memstore.IterFunc) error {
=======
func (r *Store) RangeIterator(start, end []byte, iter IterFunc) error

func (r *Store) NewBatch() MemBatch
func (r *Store) ExecuteBatch(batch MemBatch) error

func (r *Store) Close() error

func (r *Store) Get(key []byte) ([]byte, error) {
	return nil, nil
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

func (w *Store) NewBatch() kvstore.KVBatch {
	return &batch{}
}

func (w *Store) ExecuteBatch(kvstore.KVBatch) error {
>>>>>>> upstream/master
	return nil
}

func (r *Store)NewBatch() memstore.MemBatch {
	return &batch{}
}

func (r *Store)ExecuteBatch(batch memstore.MemBatch) error {
	return nil
}

func (r *Store)Close() error {
	return nil
}

<<<<<<< HEAD

type batch struct{}

func (i *batch) Set(key []byte, val interface{})   {}
func (i *batch) Delete(key []byte)     {}
func (i *batch) Reset()                {}
func (i *batch) Operations() []memstore.Operation { return nil }
func (i *batch) Close() error          { return nil }
=======
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
>>>>>>> upstream/master
