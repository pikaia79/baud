package lower

import (
	"testing"
	"github.com/tiglabs/baudengine/kernel/analysis"
)

func TestLower(t *testing.T) {
	f := New()
	set := f.Filter(analysis.TokenSet{&analysis.Token{Term: []byte("AbCd你好")}})
	if len(set) != 1 {
		t.Fatal("test fail")
	}
	if string(set[0].Term) != "abcd你好" {
		t.Fatal("test fail")
	}
	set = f.Filter(analysis.TokenSet{&analysis.Token{Term: []byte("AbΣCȺdΣ")}})
	if len(set) != 1 {
		t.Fatal("test fail")
	}
	if string(set[0].Term) != "abσcⱥdς" {
		t.Fatal("test fail")
	}
}

