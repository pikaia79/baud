package chinese

import (
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/yanyiwu/gojieba"
	"github.com/tiglabs/baudengine/kernel/config"
)

type ZhTokenizer struct {
	tokenizer     *gojieba.Jieba
}

var _ analysis.Tokenizer = &ZhTokenizer{}

func NewZh() *ZhTokenizer {
	dictpath := config.GetWordDictPath("baud_zh.dict")
	hmmpath := config.GetWordDictPath("hmm_model.utf8")
	userdictpath := config.GetWordDictPath("user.dict")
	idf := config.GetWordDictPath("idf.utf8")
	stop_words := config.GetWordDictPath("stop.dict")
	return NewZhTokenizer(dictpath, hmmpath, userdictpath, idf, stop_words)
}


func NewZhTokenizer(dictpath, hmmpath, userdictpath, idf, stop_words string) *ZhTokenizer {
	x := gojieba.NewJieba(dictpath, hmmpath, userdictpath, idf, stop_words)
	return &ZhTokenizer{x}
}

func (x *ZhTokenizer) Free() {
	x.tokenizer.Free()
}

func (x *ZhTokenizer) Tokenize(input []byte) analysis.TokenSet {
	result := make(analysis.TokenSet, 0)
	pos := 1
	words := x.tokenizer.Tokenize(string(input), gojieba.SearchMode, false)
	for _, word := range words {
		token := analysis.Token{
			Term:     []byte(word.Str),
			Start:    word.Start,
			End:      word.End,
			Position: pos,
			Type:     analysis.Numeric,
		}
		result = append(result, &token)
		pos++
	}
	return result
}
