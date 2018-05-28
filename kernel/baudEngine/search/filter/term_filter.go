package filter

import (
	"github.com/tiglabs/baudengine/kernel/index"
	"github.com/tiglabs/baudengine/kernel/search/result"
)

type TermFilter struct {
	fieldId    uint32
	matchTerm  []byte
}

func (t *TermFilter) Fields() []uint32 {
	return []uint32{t.fieldId}
}

func (t *TermFilter) FilterTerm(reader index.Index) ([]*result.DocumentMatch, error) {

}
