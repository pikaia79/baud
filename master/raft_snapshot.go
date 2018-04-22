package master

import (
	"errors"
	"github.com/gogo/protobuf/proto"
	raftproto "github.com/tiglabs/raft/proto"
	"io"
	"util/raftkvstore"
	"proto/masterpb"
)

var (
	errCorruptData = errors.New("corrupt data")
)

type RaftSnapshot struct {
	snap       raftkvstore.Snapshot
	applyIndex uint64
	iter       raftkvstore.Iterator
}

func NewRaftSnapshot(snap raftkvstore.Snapshot, applyIndex uint64, beginKey, endKey []byte) *RaftSnapshot {
	s := &RaftSnapshot{
		snap:       snap,
		applyIndex: applyIndex,
		iter:       snap.NewIterator(beginKey, endKey),
	}

	return s
}

func (s *RaftSnapshot) Next() ([]byte, error) {
	err := s.iter.Error()
	if err != nil {
		return nil, err
	}

	hasNext := s.iter.Next()
	if !hasNext {
		return nil, io.EOF
	}
	kvPair := &masterpb.RaftKvPair{
		Key:        s.iter.Key(),
		Value:      s.iter.Value(),
		ApplyIndex: s.ApplyIndex(),
	}

	return proto.Marshal(kvPair)
}

func (s *RaftSnapshot) ApplyIndex() uint64 {
	return s.applyIndex
}

func (s *RaftSnapshot) Close() {
	s.iter.Release()
	s.snap.Release()
}

type SnapshotKVIterator struct {
	rawIter raftproto.SnapIterator
}

func NewSnapshotKVIterator(rawIter raftproto.SnapIterator) *SnapshotKVIterator {
	return &SnapshotKVIterator{
		rawIter: rawIter,
	}
}

func (i *SnapshotKVIterator) Next() (kvPair *masterpb.RaftKvPair, err error) {
	var data []byte
	data, err = i.rawIter.Next()
	if err != nil {
		return
	}
	kvPair = &masterpb.RaftKvPair{}
	err = proto.Unmarshal(data, kvPair)
	if err != nil {
		return nil, err
	}
	return kvPair, nil
}
