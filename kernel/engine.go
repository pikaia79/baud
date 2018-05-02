package kernel

import (
	"github.com/tiglabs/baudengine/kernel/document"
)

type Engine interface {
	// for raft server init
	GetLastApplyID() (uint64, error)
	AddDocument(doc *document.Document, applyId uint64) error
	UpdateDocument(doc *document.Document, upsert bool, applyId uint64) (found bool, err error)
	DeleteDocument(docID []byte, applyId uint64) (int, error)
	// _source, _all as system field
	GetDocument(docID []byte, fields []string) (map[string]interface{}, bool)
	Close() error
	// TODO search
}
