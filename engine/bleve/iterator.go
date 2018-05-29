package bleve

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/tiglabs/baudengine/engine"
)

var _ engine.Iterator = &Iterator{}

type Filter interface {
	// return true for filter key
	Filter(key []byte) bool
}

type Iterator struct {
	iter  store.KVIterator
	filters []Filter
}

func (iter *Iterator) Close() error {
	if iter == nil {
		return nil
	}
	return iter.iter.Close()
}

func (iter *Iterator) Next() {
	if iter == nil {
		return
	}
	Loop:
	iter.iter.Next()
	for _, f := range iter.filters {
		if f.Filter(iter.iter.Key()) {
			goto Loop
		}
	}
}

func (iter *Iterator) Valid() bool {
	if iter == nil {
		return false
	}
	return iter.iter.Valid()
}

func (iter *Iterator) Key() []byte {
	if iter == nil {
		return nil
	}
	return  iter.iter.Key()
}

func (iter *Iterator) Value() []byte {
	if iter == nil {
		return nil
	}
	return  iter.iter.Value()
}