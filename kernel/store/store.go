package store

import "github.com/tiglabs/baud/document"

// 一个索引需要创建一个store,这个store是一个逻辑store,
type Store interface {
	// 写入一个文档
    InsertDoc(doc *document.Document) error
	// 根据doc ID查询一个文档
	QueryDoc(slotID uint32, id string) (*document.Document, error)
	MultiQueryDoc(slotID uint32, ids []string) ([]*document.Document, error)
	// 删除一个文档
	DeleteDoc(slotID uint32, id string) error
	Close()
}
