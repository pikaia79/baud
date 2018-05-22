package unicode

import (
	"testing"
	"os/exec"
	"strings"
	"os"
	"fmt"
)

func TestTokenize(t *testing.T) {
	//word := "I love you 中国,天安门很壮观"
	//sets := NewUnicodeTokenizer().Tokenize([]byte(word))
	//fmt.Println(sets)
}

func init() {
	fmt.Println(getCurrentPath())
	//config.SetWordDictPath("baud_zh.dict", "")
}


func getCurrentPath() string {
	s, err := exec.LookPath(os.Args[0])
	checkErr(err)
	i := strings.LastIndex(s, "\\")
	path := string(s[0 : i+1])
	return path
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}