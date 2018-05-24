package unicode

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
	"github.com/tiglabs/baudengine/util/bytes"
	"github.com/tiglabs/baudengine/kernel/analysis/tokenizer/chinese"
	"github.com/tiglabs/baudengine/kernel/analysis/segment"
)

const Name = "unicode"

type UnicodeTokenizer struct {
	zhTokneizer analysis.Tokenizer
}

var _ analysis.Tokenizer = &UnicodeTokenizer{}

func NewUnicodeTokenizer() *UnicodeTokenizer {
	return &UnicodeTokenizer{}
}

func (ut *UnicodeTokenizer) Tokenize(input []byte) analysis.TokenSet {
	if ut.zhTokneizer == nil {
		ut.zhTokneizer = chinese.NewZh()
	}
	segments := segment.NewSegmenter().TextSegment(input)
	pos := 1
	sets := make(analysis.TokenSet, 0, len(segments))
	for _, s := range segments {
		switch s.Type {
		case segment.Han:
			_sets := ut.zhTokneizer.Tokenize(s.Text)
			for _, set := range _sets {
				set.Start = set.Start + s.Start
				set.End = set.End + s.Start
				set.Position = pos
				pos++
			}
			sets = append(sets, _sets...)
		default:
			sets = append(sets, &analysis.Token{
				Start: s.Start,
				End: s.End,
				Type: analysis.Numeric,
				Position: pos,
				Term: bytes.CloneBytes(s.Text),
			})
			pos++
		}
	}

	return nil
}


func init() {
	registry.RegisterTokenizer(Name, NewUnicodeTokenizer())
}

