package fast

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
	"github.com/tiglabs/baudengine/kernel/analysis/filter/stop"
	"github.com/tiglabs/baudengine/kernel/analysis/filter/lower"
	"github.com/tiglabs/baudengine/kernel/analysis/tokenizer/fast"
)

const Name = "fast"

var _ analysis.Analyzer = &Analyzer{}

type Analyzer struct {
	tokenizer analysis.Tokenizer
	filters []analysis.TokenFilter
}

func New() *Analyzer {
	return &Analyzer{}
}

func (a *Analyzer)Analyze(input []byte) analysis.TokenSet {
	if a.tokenizer == nil {
		a.tokenizer = registry.GetTokenizer(fast.Name)
	}
	if a.filters == nil {
		a.filters = append(a.filters, registry.GetTokenFilter(lower.Name))
		a.filters = append(a.filters, registry.GetTokenFilter(stop.Name))
	}
	tokensets := a.tokenizer.Tokenize(input)
	for _, filter := range a.filters {
		tokensets = filter.Filter(tokensets)
	}
	return tokensets
}

func init() {
	registry.RegisterAnalyzer(Name, New())
}
