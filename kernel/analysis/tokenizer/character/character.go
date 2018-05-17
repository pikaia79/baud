package character

import (
	"unicode/utf8"

	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/analysis/filter/character"
)

type Tokenizer struct {
	filter  analysis.CharFilter
}

func NewCharTokenizer(f character.FilterOutFunc) *Tokenizer {
	return &Tokenizer{filter: character.New(f)}
}

func (t *Tokenizer) Tokenize(data []byte) analysis.TokenSet {
	offset := 0
	start := 0
	end := 0
	pos := 0
	var sets analysis.TokenSet
	for offset < len(data) {
		r, size := utf8.DecodeRune(data[offset:])
		if r == utf8.RuneError {
			return sets
		}
		offset += size
		// not char
		if t.filter.Filter(r){
			if end == start {
				start += size
				end += size
				continue
			}
			if end > start {
				// Complete token
				sets = append(sets, &analysis.Token{
					Start: start,
					End: end,
					Term: data[start:end],
					Position: pos,
				})
				pos++
				// next index
				end += size
				start = end
			}
		} else {
			end += size
		}
	}
	if end > start {
		sets = append(sets, &analysis.Token{
			Start: start,
			End: end,
			Term: data[start:end],
			Position: pos,
		})
	}
	return sets
}

