package index

import (
	"errors"
	"encoding/binary"

	"github.com/tiglabs/baudengine/kernel/store/kvstore"
	"github.com/tiglabs/baudengine/kernel"
)

var _ kernel.Snapshot = &DocSnapshot{}

type DocSnapshot struct {
	snap     kvstore.Snapshot
}

func (ds *DocSnapshot)GetLastApplyID() (uint64, error) {
	v, err := ds.snap.Get(RAFT_APPLY_ID)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("invalid raft apply ID value")
	}
	return binary.BigEndian.Uint64(v), nil
}

func (ds *DocSnapshot)NewIterator() kernel.Iterator {
	iter := ds.snap.PrefixIterator([]byte{byte(KEY_TYPE_F)})
	return &DocIterator{iter: iter}
}

func (ds *DocSnapshot)Close() {
	ds.snap.Close()
}

var _ kernel.Iterator = &DocIterator{}

type DocIterator struct {
	iter     kvstore.KVIterator
}

// Next will advance the iterator to the next key
func(di *DocIterator) Next() {
	di.iter.Next()
}

func(di *DocIterator) Key() []byte {
	return di.iter.Key()
}

func(di *DocIterator) Value() []byte {
	return di.iter.Value()
}

// Valid returns whether or not the iterator is in a valid state
func(di *DocIterator) Valid() bool {
	return di.iter.Valid()
}

// Close closes the iterator
func(di *DocIterator) Close() error {
	return di.iter.Close()
}
