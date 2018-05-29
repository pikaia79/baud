package index

import (
	"errors"
	"encoding/binary"

	"github.com/tiglabs/baudengine/kernel/store/kvstore"
	"github.com/tiglabs/baudengine/kernel"
)

var _ engine.Snapshot = &Snapshot{}

type Snapshot struct {
	snap     kvstore.Snapshot
}

func (ds *Snapshot)GetApplyID() (uint64, error) {
	v, err := ds.snap.Get(RAFT_APPLY_ID)
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
	iter := ds.snap.RangeIterator(nil, nil)
	return &Iterator{iter: iter, filters: []Filter{&RaftFilter{}}}
}

func (ds *Snapshot)Close() error {
	return ds.snap.Close()
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
