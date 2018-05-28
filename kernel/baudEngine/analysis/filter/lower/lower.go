package lower

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"unicode/utf8"
	"unicode"
)

const Name = "lower"

var _ analysis.TokenFilter = &LowerFilter{}

type LowerFilter struct {

}

func New() *LowerFilter {
	return &LowerFilter{}
}

func (l *LowerFilter) Filter(set analysis.TokenSet) analysis.TokenSet {
	for _, token := range set {
		// copy the term after to lower
		token.Term = toLowerCopy(token.Term)
	}
	return set
}

func toLowerCopy(s []byte) []byte {
	maxbytes := len(s) // length of b
	nbytes := 0        // number of bytes encoded in b
	b := make([]byte, maxbytes)
	for i := 0; i < len(s); {
		wid := 1
		r := rune(s[i])
		if r >= utf8.RuneSelf {
			r, wid = utf8.DecodeRune(s[i:])
		}
		r = unicode.ToLower(r)
		if r >= 0 {
			if r == 'σ' && i+2 == len(s) {
				r = 'ς'
			}
			rl := utf8.RuneLen(r)
			if rl < 0 {
				rl = len(string(utf8.RuneError))
			}
			if nbytes+rl > maxbytes {
				// Grow the buffer.
				maxbytes = maxbytes*2 + utf8.UTFMax
				nb := make([]byte, maxbytes)
				copy(nb, b[0:nbytes])
				b = nb
			}
			nbytes += utf8.EncodeRune(b[nbytes:maxbytes], r)
		}
		i += wid
	}
	return b[0:nbytes]

}