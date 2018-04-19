package kernel

import "github.com/tiglabs/baud/kernel/document"

type Engine interface {
	AddDocuments(docs []*document.Document) error
	UpdateDocuments(docs []*document.Document) error
	DeleteDocuments(docIDs []string) (int, error)

	// source set true means need return _source
	GetDocument(docID string, fields []string, source bool) (*document.Document, bool)

	// TODO search
}
