package null

import (
	"github.com/tiglabs/baud/kernel/store/kvstore"
)

var _ kvstore.KVStore = &Store{}

type Store struct{}

func New() (kvstore.KVStore, error) {
	return &Store{}, nil
}

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
	return nil
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


func (i *Store) Reader() (kvstore.KVReader, error) {
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

func (i *batch) Set(key, val []byte)   {}
func (i *batch) Delete(key []byte)     {}
func (i *batch) Merge(key, val []byte) {}
func (i *batch) Reset()                {}
func (i *batch) Operations() []kvstore.Operation { return nil }
func (i *batch) Close() error          { return nil }
