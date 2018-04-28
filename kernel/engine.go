package kernel

import (
	"github.com/tiglabs/baudengine/kernel/document"
)

type Option struct {
}

type Engine interface {
	AddDocument(doc *document.Document, ops ...*Option) error
	UpdateDocument(doc *document.Document, upsert bool, ops ...*Option) (found bool, err error)
	DeleteDocument(docID []byte, ops ...*Option) (int, error)
	// _source, _all as system field
	GetDocument(docID []byte, fields []string) (map[string]interface{}, bool)
	Close() error
	// TODO search
}
