package keyword

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
	"github.com/tiglabs/baudengine/kernel/analysis/tokenizer/keyword"
)

const Name = "keyword"

// The keyword analyzer is a “noop” analyzer that accepts whatever text it is given and outputs
// the exact same text as a single term.
type Analyzer struct {
	tokenizer analysis.Tokenizer
}

func New() *Analyzer {
	return &Analyzer{tokenizer: keyword.New()}
}

func (a *Analyzer)Analyze(input []byte) analysis.TokenSet {
	return a.tokenizer.Tokenize(input)
}

func init() {
	registry.RegisterAnalyzer(Name, New())
}
