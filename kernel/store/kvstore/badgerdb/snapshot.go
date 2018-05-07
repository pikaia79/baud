package badgerdb

import (
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
)

type Snapshot struct {
	close  sync.Once
	tx     *badger.Txn
}

func (r *Snapshot) Get(key []byte) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	var rv []byte
	v, err := r.tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rv, err = v.ValueCopy(rv)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (r *Snapshot) MultiGet(keys [][]byte) ([][]byte, error) {
	if r == nil {
		return nil, nil
	}
	return kvstore.MultiGet(r, keys)
}

func (r *Snapshot) PrefixIterator(prefix []byte) kvstore.KVIterator {
	if r == nil {
		return nil
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := r.tx.NewIterator(opts)
	rv := &Iterator{
		// we must not set tx here
		iter: it,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

func (r *Snapshot) RangeIterator(start, end []byte) kvstore.KVIterator {
	if r == nil {
		return nil
	}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := r.tx.NewIterator(opts)
	rv := &Iterator{
		// we must not set tx here
		iter: it,
		start:  start,
		end:    end,
	}

	rv.Seek(start)
	return rv
}

func (r *Snapshot) Close() error {
	if r == nil {
		return nil
	}
	r.close.Do(func() {
		if r.tx != nil {
			r.tx.Discard()
		}
	})
	return nil
}
