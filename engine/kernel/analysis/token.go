package analysis

import "fmt"

type TokenType int

const (
	Numeric TokenType = iota
	DateTime
	Boolean
	Text
	KeyWord
)

type TokenSet  []*Token

type Token struct {
	Start int   `json:"start"`
	End  int    `json:"end"`
	Term []byte `json:"term"`

	Position int       `json:"position"`
	Type     TokenType `json:"type"`
}

func (t *Token) String() string {
	return fmt.Sprintf("start: %d, end: %d, term: %s, position %d", t.Start, t.End, string(t.Term), &t.Position)
}

type Tokenizer interface {
	Tokenize([]byte) TokenSet
}

type CharFilter interface {
	// filter char return true if the input need filter out
	Filter(rune) bool
}

type TextFilter interface {
	Filter([]rune) []rune
}

type TokenFilter interface {
	Filter(TokenSet) TokenSet
}
