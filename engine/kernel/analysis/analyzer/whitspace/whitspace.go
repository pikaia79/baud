package whitspace

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/analysis/tokenizer/character"
	"github.com/tiglabs/baudengine/kernel/registry"
)

var _ analysis.Analyzer = &Analyzer{}

const Name = "whitspace"
const whitSpaceRune rune = 32

type Analyzer struct {
	tokenizer analysis.Tokenizer
}

func New() *Analyzer {
	return &Analyzer{tokenizer: character.NewCharTokenizer(func(r rune) bool {
		return r == whitSpaceRune
	})}
}

func (a *Analyzer)Analyze(input []byte) analysis.TokenSet {
	return a.tokenizer.Tokenize(input)
}

func init() {
	registry.RegisterAnalyzer(Name, New())
}
