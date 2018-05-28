package character

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
)

var _ analysis.CharFilter = &CharacterFilter{}

type FilterOutFunc func(r rune) bool

type CharacterFilter struct {
	filterOut FilterOutFunc
}

func New(f FilterOutFunc) *CharacterFilter {
	return &CharacterFilter{filterOut: f}
}

func (cf *CharacterFilter) Filter(r rune) bool {
	return cf.filterOut(r)
}


