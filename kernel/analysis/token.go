package analysis

type TokenType int

const (
	Numeric TokenType = iota
	DateTime
	Boolean
	Text
	IPv4
	KeyWord
)

type Token struct {
	// Start specifies the byte offset of the beginning of the term in the
	// field.
	Start int   `json:"start"`

	// End specifies the byte offset of the end of the term in the field.
	End  int    `json:"end"`
	Term []byte `json:"term"`

	// Position specifies the 1-based index of the token in the sequence of
	// occurrences of its term in the field.
	Position int       `json:"position"`
	Type     TokenType `json:"type"`
	KeyWord  bool      `json:"keyword"`
}

