package kernel

import (
	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/proto/metapb"
)

type Option struct {
}

type Engine interface {
	AddDocuments(docs []*document.Document, ops ...*Option) error
	UpdateDocuments(docs []*document.Document, ops ...*Option) error
	DeleteDocuments(docIDs []string, ops ...*Option) (int, error)
	// _source, _all as system field
	GetDocument(docID *metapb.DocID, fields []string) (map[string]interface{}, bool)
	Close() error
	// TODO search
}
