package character

import (
	"testing"
	"unicode"
)

func TestTokenize(t *testing.T) {
	tokenizer := NewCharTokenizer(func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	sets := tokenizer.Tokenize([]byte("&&abcd我们??bb$$哈哈哈"))
	if len(sets) != 3 {
		t.Fatalf("test failed sets size %d", len(sets))
	}
	if string(sets[0].Term) != "abcd我们" || sets[0].Position != 0{
		t.Fatal("test failed")
	}
	if string(sets[1].Term) != "bb" || sets[1].Position != 1{
		t.Fatal("test failed")
	}
	if string(sets[2].Term) != "哈哈哈" || sets[2].Position != 2{
		t.Fatal("test failed")
	}
}
