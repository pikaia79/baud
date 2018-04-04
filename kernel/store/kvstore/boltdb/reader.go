package boltdb

import (
	"github.com/boltdb/bolt"
	"baud/kernel/store/kvstore"
)

type Reader struct {
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	if r == nil {
		return nil, nil
	}
	var rv []byte
	v := r.bucket.Get(key)
	if v != nil {
		rv = make([]byte, len(v))
		copy(rv, v)
	}
	return rv, nil
}

func (r *Reader) MultiGet(keys [][]byte) ([][]byte, error) {
	if r == nil {
		return nil, nil
	}
	return kvstore.MultiGet(r, keys)
}

func (r *Reader) PrefixIterator(prefix []byte) kvstore.KVIterator {
	if r == nil {
		return nil
	}
	cursor := r.bucket.Cursor()

	rv := &Iterator{
		// we must not set tx here
		cursor: cursor,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

func (r *Reader) RangeIterator(start, end []byte) kvstore.KVIterator {
	if r == nil {
		return nil
	}
	cursor := r.bucket.Cursor()

	rv := &Iterator{
		// we must not set tx here
		cursor: cursor,
		start:  start,
		end:    end,
	}

	rv.Seek(start)
	return rv
}

func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	return r.tx.Rollback()
}
