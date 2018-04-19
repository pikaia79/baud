package kernel

import "github.com/tiglabs/baud/kernel/document"

type Engine interface {
	AddDocuments(docs []*document.Document) error
	UpdateDocuments(docs []*document.Document) error
	DeleteDocuments(docIDs []string) (int, error)

	// _source, _all as system field
	GetDocument(docID string, fields []string) (*document.Document, bool)

	// TODO search
}
