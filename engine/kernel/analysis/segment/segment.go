package segment

import (
	"unicode/utf8"
	"unicode"
	"fmt"
	"strconv"
	"github.com/tiglabs/baudengine/kernel/analysis/dict/symbol"
)

type SegmentType int

const (
	None  SegmentType = iota
	Han
	En
	Korean
	Japanese
	Number
	Other
)

var SegmentType_Name = map[int32]string{
	0: "None",
	1: "Han",
	2: "En",
	3: "Korean",
	4: "Japanese",
	5: "Number",
	6: "Other",
}

func (st SegmentType) String() string {
	if s, ok := SegmentType_Name[int32(st)]; ok {
		return s
	}
	return strconv.Itoa(int(st))
}

const (
	prime rune = '\''

	minHan rune = '\u4e00'
	maxHan rune = '\u9fa5'
	minKorean1 rune = 0x3130
    maxKorean1 rune = 0x318F
    minKorean2 rune = 0xAC00
    maxKorean2 rune = 0xD7A3
    minJapanese rune = '\u0800'
    maxJapanese rune = '\u4e00'
)

type Segment struct {
	Type     SegmentType
	Start    int
	End      int
	Text     []byte
}

func NewSegment(tpe SegmentType, start, end int) *Segment{
	return &Segment{Type: tpe, Start: start, End: end}
}

func (s *Segment) String() string {
	return fmt.Sprintf("type %s start %d end %d text %s", s.Type, s.Start, s.End, string(s.Text))
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
	var _type SegmentType = None
	for i = 0; i < len(text); {
		r, size := utf8.DecodeRune(text[i:])
		if r == utf8.RuneError {
			return sets
		}
		if !unicode.IsPrint(r) || unicode.IsSymbol(r) || unicode.IsSpace(r) || symbol.IsSymbol(r) {
			_type = None
		} else if size <= 2 && unicode.IsLetter(r) {
			// TODO more language
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
			// TODO more no language character
			fmt.Println("other ", r, '(')
			_type = Other
		}
		if segment != nil {
			if segment.Type != _type {
				segment.End = i
				sets = append(sets, segment)
				segment = nil
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
