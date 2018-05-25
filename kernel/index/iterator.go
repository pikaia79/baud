package index

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/kernel"
	"github.com/blevesearch/bleve/index/store"
)

var _ kernel.Iterator = &Iterator{}

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

func (iter *Iterator) Key() metapb.Key {
	if iter == nil {
		return nil
	}
	return  metapb.Key(iter.iter.Key())
}

func (iter *Iterator) Value() metapb.Value {
	if iter == nil {
		return nil
	}
	return  metapb.Key(iter.iter.Value())
}