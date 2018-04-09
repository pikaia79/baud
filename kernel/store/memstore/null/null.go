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
func (r *Store)Put(key []byte, value interface{}) error {
	return nil
}

func (r *Store)Get(key []byte) (interface{}, error) {
	return nil, nil
}

func (r *Store)Delete(key []byte) error {
	return nil
}

// PrefixIterator will visit all K/V pairs with the provided prefix.
// Returns an error if the store is closed or the tx is closed.
// Because it will block the write operation, users should
// not do slow operation as much as possible.
func (r *Store)PrefixIterator(prefix []byte, iter memstore.IterFunc) error {
	return nil
}

// RangeIterator will visit all K/V pairs >= start AND < end
// Returns an error if the store is closed or the tx is closed.
// Because it will block the write operation, users should
// not do slow operation as much as possible.
func (r *Store)RangeIterator(start, end []byte, iter memstore.IterFunc) error {
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


type batch struct{}

func (i *batch) Set(key []byte, val interface{})   {}
func (i *batch) Delete(key []byte)     {}
func (i *batch) Reset()                {}
func (i *batch) Operations() []memstore.Operation { return nil }
func (i *batch) Close() error          { return nil }
