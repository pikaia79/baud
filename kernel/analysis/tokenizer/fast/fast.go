package fast

import (
	"os"
	"fmt"
	"bufio"
	"io"
	"strings"
	"strconv"

	"github.com/tiglabs/baudengine/kernel/analysis"
	acdat "github.com/heidawei/AhoCorasickDoubleArrayTrie/ACDAT"
	"github.com/tiglabs/baudengine/util/bytes"
)

const Name = "fast"

type FastZhTokenizer struct {
	// may be more dict
	dictPaths []string
	dict     *acdat.AhoCorasickDoubleArrayTrie
}

var _ analysis.Tokenizer = &FastZhTokenizer{}


func NewFastZhTokenizer(dictpaths []string) *FastZhTokenizer {
	return &FastZhTokenizer{dictPaths: dictpaths}
}

// dict format: word or word [type freq ......] or word [freq type ......]

func (x *FastZhTokenizer) loadDicts() {
	strMap := acdat.NewStringTreeMap()
	for _, path := range x.dictPaths {
		func(_path string) {
			f, err := os.Open(_path)
			if err != nil {
				panic(fmt.Sprintf("open dict failed, err %v", err))
				return
			}
			defer f.Close()

			r := bufio.NewReader(f)
		LOAD:
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					if err == io.EOF {
						break
					}
					panic(fmt.Sprintf("read dict failed, err %v", err))
				}
				es := strings.Fields(l)
				if len(es) == 0 {
					continue
				}
				// invalid line
				if len(es) % 2 ==  0 {
					continue
				}
				w := NewWord([]byte(es[0]))
				for i := 1; i + 1 < len(es);  {
					var type_ string
					var freq int
					// fomat: word [freq type ......]
					if byte(es[i][0]) >= '0' && byte(es[i][0]) <= '9' {
						type_ = es[i+1]
						freq, err = strconv.Atoi(es[i])
						if err != nil {
							continue LOAD
						}
					} else {
						// fomat: word [type freq ......]
						type_ = es[i]
						freq, err = strconv.Atoi(es[i+1])
						if err != nil {
							continue LOAD
						}
					}

					w.addProperty(&WordProperty{type_: type_, freq: freq})
					i += 2
				}
				strMap.Add(es[0], w)
			}
		}(path)
	}
	x.dict.Build(strMap)
}

func (x *FastZhTokenizer) Tokenize(input []byte) analysis.TokenSet {
	// lazy load
	if x.dict == nil {
		x.loadDicts()
	}
	result := make(analysis.TokenSet, 0)
	pos := 1
	x.dict.ParseBytesWithIter(input, func(begin, end int, v interface{}) {
		token := analysis.Token{
			Term:     bytes.CloneBytes(v.(*DictWord).word),
			Start:    begin,
			End:      end,
			Position: pos,
			Type:     analysis.Numeric,
		}
		result = append(result, &token)
		pos++
	})
	return result
}

type WordProperty struct {
	//
	type_      string
	freq       int
}

type DictWord struct {
	word    []byte
    properties []*WordProperty
}

func NewWord(w []byte) *DictWord {
	return &DictWord{word: w}
}

func (w *DictWord) addProperty(p *WordProperty) *DictWord {
	w.properties =  append(w.properties, p)
	return w
}
