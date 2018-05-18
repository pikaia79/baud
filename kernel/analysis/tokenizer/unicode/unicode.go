package unicode

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
)

const Name = "unicode"

type UnicodeTokenizer struct {
}

var _ analysis.Tokenizer = &UnicodeTokenizer{}

func NewUnicodeTokenizer() *UnicodeTokenizer {
	return &UnicodeTokenizer{}
}

func (rt *UnicodeTokenizer) Tokenize(input []byte) analysis.TokenSet {


	return nil
}


func init() {
	registry.RegisterTokenizer(Name, NewUnicodeTokenizer())
}

