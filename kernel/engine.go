package kernel

import (
	"github.com/tiglabs/baudengine/kernel/document"
	"golang.org/x/net/context"
)

type Snapshot interface {
	GetLastApplyID() (uint64, error)
	NewIterator() Iterator
	Close()
}

type Iterator interface {
	// Next will advance the iterator to the next key
	Next()

	Key() []byte

	Value() []byte

	// Valid returns whether or not the iterator is in a valid state
	Valid() bool

	// Close closes the iterator
	Close() error
}

type Engine interface {
	// for raft server init
	SetApplyID(uint64) error
	GetApplyID() (uint64, error)
	GetDocSnapshot() (Snapshot, error)
	ApplyDocSnapshot(ctx context.Context, iter Iterator) error
	AddDocument(ctx context.Context, doc *document.Document, applyID uint64) error
	UpdateDocument(ctx context.Context, doc *document.Document, upsert bool, applyID uint64) (found bool, err error)
	DeleteDocument(ctx context.Context, docID []byte, applyID uint64) (int, error)
	// _source, _all as system field
	GetDocument(ctx context.Context, docID []byte, fields []string) (map[string]interface{}, bool)
	Close() error
	// TODO search
}
