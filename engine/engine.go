package engine

import (
	"context"
	"io"
)

type DOC_ID []byte

func (d DOC_ID) ToString() string {
	return string(d)
}

func (d DOC_ID) ToBytes() []byte {
	return []byte(d)
}

type DOCUMENT map[string]interface{}

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
	Key() []byte
	Value() []byte
}

// Reader is the read interface to an engine's data.
type Reader interface {
	io.Closer
	GetApplyID() (uint64, error)
	GetDocument(ctx context.Context, docID DOC_ID) (DOCUMENT, bool)
	Search(ctx context.Context, req *SearchRequest)(*SearchResult, error)
}

// Writer is the write interface to an engine's data.
type Writer interface {
	SetApplyID(uint64) error
	AddDocument(ctx context.Context, docID DOC_ID, doc interface{}) error
	UpdateDocument(ctx context.Context, docID DOC_ID, doc interface{}, upsert bool) (found bool, err error)
	DeleteDocument(ctx context.Context, docID DOC_ID) (int, error)
}

// ReadWriter is the read/write interface to an engine's data.
type ReadWriter interface {
	Reader
	Writer
}

// Batch is the interface for batch operations.
type Batch interface {
	Writer
	Commit() error
	Rollback() error
}

// Engine is the interface that wraps the core operations of a document store.
type Engine interface {
	ReadWriter
	NewWriteBatch() Batch
	NewSnapshot() (Snapshot, error)
	ApplySnapshot(ctx context.Context, iter Iterator) error
}
