package segment

import (
	"testing"
	"unicode/utf8"
	"fmt"
	"unicode"
)

func TestSegment(t *testing.T) {
	word := "你好 祖国 한국어にすることだと1234.23, I love you &$(Beijing) et Washington conviennent de ne pas se livrer à une guerre commerciale"
	ss := NewSegmenter().TextSegment([]byte(word))
	if len(ss) != 21 {
		for _, s := range ss {
			t.Logf("segment %s", s)
		}
		t.Fatalf("test failed %d", len(ss))
	}
	if string(ss[0].Text) != "你好" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[1].Text) != "祖国" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[2].Text) != "한국어" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[3].Text) != "にすることだと" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[4].Text) != "1234" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[5].Text) != "23" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[6].Text) != "I" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[7].Text) != "love" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[8].Text) != "you" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[9].Text) != "Beijing" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[10].Text) != "et" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[11].Text) != "Washington" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[12].Text) != "conviennent" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[13].Text) != "de" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[14].Text) != "ne" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[15].Text) != "pas" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[16].Text) != "se" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[17].Text) != "livrer" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[18].Text) != "une" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[19].Text) != "guerre" {
		t.Fatalf("test failed, %s", ss[0])
	}
	if string(ss[20].Text) != "commerciale" {
		t.Fatalf("test failed, %s", ss[0])
	}

}

func TestHan(t *testing.T) {
	text := []byte("你好")
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRune(text[i:])
		if r == utf8.RuneError {
			t.Fatal("test failed")
		}
		if r >= minHan && r <= maxHan {
			fmt.Println("han ", string(text[i:i+size]))
		} else {
			t.Fatalf("test failed %d", r)
		}
		i += size
	}
}

func TestNumber(t *testing.T) {
	text := []byte("12345")
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRune(text[i:])
		if r == utf8.RuneError {
			t.Fatal("test failed")
		}
		if unicode.IsNumber(r) {
			fmt.Println("number ", string(text[i:i+size]))
		} else {
			t.Fatalf("test failed %d", r)
		}
		i += size
	}
}

func TestEn(t *testing.T) {
	text := []byte("abcdefgz Α 我们")
	for i := 0; i < len(text); {
		r, size := utf8.DecodeRune(text[i:])
		if r == utf8.RuneError {
			t.Fatal("test failed")
		}
		if unicode.IsLetter(r) {
			fmt.Println("letter ", string(text[i:i+size]))
		} else {
			t.Fatalf("test failed %d", r)
		}
		i += size
	}
}