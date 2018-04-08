package store

import (
	"github.com/tiglabs/baud/kernel/document"
)

// 一个索引需要创建一个store,这个store是一个逻辑store,
type Store interface {
	// 写入一个entity
	InsertEntity(doc *document.Entity) error
	// 根据UID查询一个entity
	QueryEntity(uid document.UID) (*document.Entity, error)
	// 删除一个entity
	DeleteEntity(uid document.UID) error
	// 写入一个entity
	InsertEdge(edge *document.Edge) error
	// 根据UID pair 查询一个edge
	QueryEdge(src, dst document.UID) (*document.Edge, error)
	// 删除一个edge
	DeleteEdge(src, dst document.UID) error
	Close()
}
