package filter

import (
	"github.com/tiglabs/baudengine/kernel/search/result"
	"github.com/tiglabs/baudengine/kernel/index"
)

type Border struct {
	Operator string
	Value    []byte
}

type RangeFilter struct {
	fieldId    uint32

	max        *Border
	min        *Border
}

func NewRangeFilter(fieldId uint32) *RangeFilter {
	return &RangeFilter{fieldId: fieldId}
}

func (r *RangeFilter) SetBorder(op string, value []byte) {
	if op == "<" || op == "<=" {
		r.max = &Border{Operator: op, Value: value}
	} else if op == ">" || op == ">=" {
		r.min = &Border{Operator: op, Value: value}
	} else {
		panic("invalid operator")
	}
}

func (r *RangeFilter) Fields() []uint32 {
	return []uint32{r.fieldId}
}

func (r *RangeFilter) FilterTerm(reader index.Index, docSet []*result.DocumentMatch) ([]*result.DocumentMatch, error) {

}