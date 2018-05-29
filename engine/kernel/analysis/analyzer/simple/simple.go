package simple

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
	"github.com/tiglabs/baudengine/kernel/analysis/tokenizer/character"
	"unicode"
)

const Name = "simple"

// The simple analyzer divides text into terms whenever it encounters a character which is not a letter.
// It lower cases all terms.
type Analyzer struct {
	tokenizer analysis.Tokenizer
}

func New() *Analyzer {
	return &Analyzer{tokenizer: character.NewCharTokenizer(func(r rune) bool {
		return !unicode.IsLetter(r)
	})}
}

func (a *Analyzer)Analyze(val []byte) analysis.TokenSet {
	return a.tokenizer.Tokenize(val)
}

func init() {
	registry.RegisterAnalyzer(Name, New())
}
