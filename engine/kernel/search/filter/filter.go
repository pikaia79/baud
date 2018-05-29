package filter

import (
	"github.com/tiglabs/baudengine/kernel/search/result"
	"github.com/tiglabs/baudengine/kernel/index"
)

type Filter interface {
	Fields() []uint32
	FilterTerm(reader index.Index, docSet []*result.DocumentMatch) ([]*result.DocumentMatch, error)
}
