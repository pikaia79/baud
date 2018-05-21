package kernel

import (
	"context"
	"io"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
)

// Snapshot is an interface for read-only snapshot in an engine.
type Snapshot interface {
	io.Closer
	GetApplyID() (uint64, error)
	NewIterator() Iterator
}

// Iterator is an interface for iterating over key/value pairs in an engine.
type Iterator interface {
	io.Closer
	Next()
	Valid() bool
	Key() metapb.Key
	Value() metapb.Value
}

// Reader is the read interface to an engine's data.
type Reader interface {
	io.Closer
	GetApplyID() (uint64, error)
	GetDocument(ctx context.Context, docID metapb.Key, fields []uint32) (map[uint32]pspb.FieldValue, bool)
}

// Writer is the write interface to an engine's data.
type Writer interface {
	SetApplyID(uint64) error
	AddDocument(ctx context.Context, doc *pspb.Document) error
	UpdateDocument(ctx context.Context, doc *pspb.Document, upsert bool) (found bool, err error)
	DeleteDocument(ctx context.Context, docID metapb.Key) (int, error)
}

// ReadWriter is the read/write interface to an engine's data.
type ReadWriter interface {
	Reader
	Writer
}

// Transaction is the interface for batch operations.
type Transaction interface {
	Writer
	Commit() error
	Rollback() error
}

// Engine is the interface that wraps the core operations of a document store.
type Engine interface {
	ReadWriter
	NewTransaction(update bool) (Transaction, error)
	NewSnapshot() (Snapshot, error)
	ApplySnapshot(ctx context.Context, iter Iterator) error
}
