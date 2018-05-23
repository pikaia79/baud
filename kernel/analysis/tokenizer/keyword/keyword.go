package keyword

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
)

const Name = "keyword"

type Tokenizer struct {

}

func New() *Tokenizer {
	return &Tokenizer{}
}

func (t *Tokenizer) Tokenize(input []byte) analysis.TokenSet {
	return analysis.TokenSet{&analysis.Token{Start: 0, End: len(input), Term: input, Position: 0, Type: analysis.KeyWord}}
}

func init() {
	registry.RegisterTokenizer(Name, New())
}
