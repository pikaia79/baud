package stop

import (
	"bufio"
	"io"
	"fmt"
	"os"

	"github.com/heidawei/gotrie/trie"
	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/kernel/registry"
	"github.com/tiglabs/baudengine/kernel/config"
)

var _ analysis.TokenFilter = &StopFilter{}

const Name = "stop"

type StopFilter struct {
	dict   *trie.Trie
}

func New() *StopFilter {
	sf := &StopFilter{dict: trie.NewTrie()}
	sf.loadDict()
	return &StopFilter{}
}

func (sf *StopFilter) loadDict() error {
	sf.dict = trie.NewTrie()
	f, err := os.Open(config.GetStopWordDictPath())
	if err != nil {
		return err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		sf.dict.ReplaceOrInsert([]byte(line), nil)
	}
	return nil
}

func (sf *StopFilter) Filter(input analysis.TokenSet) analysis.TokenSet {
	if sf.dict == nil {
		err := sf.loadDict()
		if err != nil {
			panic(fmt.Sprintf("load dict failed, err %v", err))
		}
	}
	index := 0
	for _, token := range input {
		_, isStopToken := sf.dict.Find(token.Term)
		if !isStopToken {
			input[index] = token
			index++
		}
	}

	return input[:index]
}

func init() {
	registry.RegisterTokenFilter(Name, New())
}
