package boltdb

import (
	"encoding/binary"
	"github.com/boltdb/bolt"
	"github.com/tiglabs/baud/kernel/store/kvstore"
)

type Snapshot struct {
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

func (r *Snapshot) Get(key []byte) ([]byte, error) {
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
	cursor := r.bucket.Cursor()

	rv := &Iterator{
		// we must not set tx here
		cursor: cursor,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

func (r *Snapshot) RangeIterator(start, end []byte) kvstore.KVIterator {
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

func (r *Snapshot) LastOption() (*kvstore.Option, error) {
	if r == nil {
		return nil, nil
	}

	raft := r.tx.Bucket(raftBucket)
	v := raft.Get(RAFT_APPLY_ID)
	if len(v) == 0 {
		return &kvstore.Option{}, nil
	} else {
		return &kvstore.Option{ApplyID: binary.BigEndian.Uint64(v)}, nil
	}
}

func (r *Snapshot) Close() error {
	if r == nil {
		return nil
	}
	return r.tx.Rollback()
}
