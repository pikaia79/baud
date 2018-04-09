package store

import (
	"github.com/tiglabs/baud/kernel/document"
)

type Store interface {
	// Insert entity
	InsertEntity(doc *document.Entity) error
	// Search entity by UID
	QueryEntity(uid document.UID) (*document.Entity, error)
	// Delete entity
	DeleteEntity(uid document.UID) error
	// Insert entity
	InsertEdge(edge *document.Edge) error
	// Search edge by UID
	QueryEdge(src, dst document.UID) (*document.Edge, error)
	// Delete edge
	DeleteEdge(src, dst document.UID) error
	Close()
}
