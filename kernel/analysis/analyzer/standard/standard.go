package standard

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
)

const Name = "keyword"

var _ analysis.Analyzer = &Analyzer{}

type Analyzer struct {
	tokenizer analysis.Tokenizer
	filters []analysis.TokenFilter
}

func New() *Analyzer {
	return &Analyzer{}
}

func (a *Analyzer)Analyze(input []byte) analysis.TokenSet {
	tokensets := a.tokenizer.Tokenize(input)
	for _, filter := range a.filters {
		tokensets = filter.Filter(tokensets)
	}
	return tokensets
}

func init() {
	registry.RegisterAnalyzer(Name, New())
}
