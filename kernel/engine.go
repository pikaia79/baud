package kernel

import "github.com/tiglabs/baud/kernel/document"

type Option struct {

}

type Engine interface {
	AddDocuments(docs []*document.Document, ops ...*Option) error
	UpdateDocuments(docs []*document.Document, ops ...*Option) error
	DeleteDocuments(docIDs []string, ops ...*Option) (int, error)

	// _source, _all as system field
	GetDocument(docID string, fields []string) (*document.Document, bool)

	// TODO search
}
