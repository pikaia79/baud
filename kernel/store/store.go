package store

import (
	"github.com/tiglabs/baud/kernel/object"
)

type Store interface {
	// Insert entity
	InsertEntity(doc *object.Entity) error
	// Search entity by UID
	QueryEntity(uid object.UID) (*object.Entity, error)
	// Delete entity
	DeleteEntity(uid object.UID) error
	// Insert entity
	InsertEdge(edge *object.Edge) error
	// Search edge by UID
	QueryEdge(src, dst object.UID) (*object.Edge, error)
	// Delete edge
	DeleteEdge(src, dst object.UID) error
	Close()
}
