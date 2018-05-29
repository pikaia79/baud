package result

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/index"
)

type DocumentMatch struct {
	DocId       []byte
	Tokens      map[string]*analysis.TokenFreq

	Score       float64

	Fields      map[uint32]*index.Field
}