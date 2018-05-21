package kernel

import (
	"context"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
)

type Snapshot interface {
	GetLastApplyID() (uint64, error)
	NewIterator() Iterator
	Close()
}

type Iterator interface {
	// Next will advance the iterator to the next key
	Next()

	Key() metapb.Key

	Value() metapb.Value

	// Valid returns whether or not the iterator is in a valid state
	Valid() bool

	// Close closes the iterator
	Close() error
}

type Engine interface {
	// for raft server init
	SetApplyID(uint64) error
	GetApplyID() (uint64, error)
	GetSnapshot() (Snapshot, error)
	ApplySnapshot(ctx context.Context, iter Iterator) error
	AddDocument(ctx context.Context, doc *pspb.Document, applyID uint64) error
	UpdateDocument(ctx context.Context, doc *pspb.Document, upsert bool, applyID uint64) (found bool, err error)
	DeleteDocument(ctx context.Context, docID metapb.Key, applyID uint64) (int, error)
	GetDocument(ctx context.Context, docID metapb.Key, fields []uint32) (map[uint32]pspb.FieldValue, bool)
	Close() error
	// TODO search
}
