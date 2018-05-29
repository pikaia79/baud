package bleve

import (
	"errors"
	"encoding/binary"

	"github.com/blevesearch/bleve/index/store"
	"github.com/tiglabs/baudengine/engine"
)

var _ engine.Snapshot = &Snapshot{}

type Snapshot struct {
	reader     store.KVReader
}

func (ds *Snapshot)GetApplyID() (uint64, error) {
	v, err := ds.reader.Get(RAFT_APPLY_ID)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("invalid raft apply ID value")
	}
	return binary.BigEndian.Uint64(v), nil
}

// filter raft log key
func (ds *Snapshot)NewIterator() engine.Iterator {
	iter := ds.reader.RangeIterator(nil, nil)
	return &Iterator{iter: iter, filters: []Filter{&RaftFilter{}}}
}

func (ds *Snapshot)Close() error {
	return ds.reader.Close()
}

type RaftFilter struct {}

func (f *RaftFilter) Filter(key []byte) bool {
	// we only need compare first byte
	if len(key) == 0 {
		return false
	}
	if key[0] == 'R' {
		return true
	}
	return false
}
