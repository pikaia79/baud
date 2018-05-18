package unicode

import (
	"unicode/utf8"
	"unicode"
)

const (
	None   = iota
	Han
	En
	Korean
	Japanese
	Number
	Other
)

const (
	prime rune = '\''

	minHan rune = '\u9fa5'
	maxHan rune = '\u9fa5'
	minKorean1 rune = 0x3130
    maxKorean1 rune = 0x318F
    minKorean2 rune = 0xAC00
    maxKorean2 rune = 0xD7A3
    minJapanese rune = '\u0800'
    maxJapanese rune = '\u4e00'
)

type Segment struct {
	Type     int
	Start    int
	End      int
	Text     []byte
}

func NewSegment(tpe, start, end int) *Segment{
	return &Segment{Type: tpe, Start: start, End: end}
}

type Segmenter struct {

}

func NewSegmenter() *Segmenter {
	return &Segmenter{}
}

func (s *Segmenter) TextSegment(text []byte) []*Segment {
	var sets []*Segment
	var segment *Segment
	var i int
	var _type int = None
	for i = 0; i < len(text); {
		r, size := utf8.DecodeRune(text[i:])
		if r == utf8.RuneError {
			return sets
		}
		// letter
		if size <= 2 && unicode.IsLetter(r) {
			_type = En
		} else if size <= 2 && unicode.IsNumber(r) {
			_type = Number
		} else if r >= minHan && r <= maxHan {
			_type = Han
		} else if (r >= minKorean1 && r <= maxKorean1) || (r >= minKorean2 && r <= maxKorean2) {
			_type = Korean
		} else if r >= minJapanese && r <= maxJapanese {
			_type = Japanese
		} else {
			// 不可打印
			// TODO more no language character
			if !unicode.IsPrint(r) || unicode.IsSymbol(r) {
				// do nothing
			} else {
				_type = Other
			}
		}
		if segment != nil {
			if segment.Type != _type {
				segment.End = i
				sets = append(sets, segment)
				segment = nil
				_type = None
			}
		}
		if segment == nil && _type > None {
			segment = NewSegment(_type, i, i)
		}
		if segment != nil {
			segment.Text = append(segment.Text, text[i:i+size]...)
		}
		i += size
	}
	if segment != nil {
		segment.End = i
		sets = append(sets, segment)
	}
	return sets
}
