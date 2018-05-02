package kernel

import (
	"github.com/tiglabs/baudengine/kernel/document"
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
	GetLastApplyID() (uint64, error)
	GetDocSnapshot() (Snapshot, error)
	ApplyDocSnapshot(applyId uint64, iter Iterator) error
	AddDocument(doc *document.Document, applyId uint64) error
	UpdateDocument(doc *document.Document, upsert bool, applyId uint64) (found bool, err error)
	DeleteDocument(docID []byte, applyId uint64) (int, error)
	// _source, _all as system field
	GetDocument(docID []byte, fields []string) (map[string]interface{}, bool)
	Close() error
	// TODO search
}
