package character

import (
	"unicode/utf8"

	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/analysis/filter/character"
	"github.com/tiglabs/baudengine/util/bytes"
)

type Tokenizer struct {
	filter  analysis.CharFilter
}

func NewCharTokenizer(f character.FilterOutFunc) *Tokenizer {
	return &Tokenizer{filter: character.New(f)}
}

func (t *Tokenizer) Tokenize(input []byte) analysis.TokenSet {
	var sets analysis.TokenSet
	var token *analysis.Token
	var i, pos int
	for i = 0; i < len(input); {
		r, size := utf8.DecodeRune(input[i:])
		if r == utf8.RuneError {
			return sets
		}
		if t.filter.Filter(r) {
			if token != nil {
				token.End = i
				token.Term = bytes.CloneBytes(input[token.Start: token.End])
				token.Position = pos
				sets = append(sets, token)
				token = nil
				pos++
			}
		} else {
			if token == nil {
				token = &analysis.Token{Start: i}
			}
		}
		i += size
	}
	if token != nil {
		token.End = i
		token.Term = bytes.CloneBytes(input[token.Start: token.End])
		token.Position = pos
		sets = append(sets, token)
	}
	return sets
}

