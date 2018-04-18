package bytes

import (
	"bytes"
	"testing"
)

func TestStringToByte(t *testing.T) {
	s := "hello"
	b := StringToByte(s)
	if !bytes.Equal(b, []byte("hello")) {
		t.Errorf("StringToBytes result error: %s->%s", s, string(b))
	}

	b1 := []byte("test")
	s1 := ByteToString(b1)
	if s1 != "test" {
		t.Errorf("BytesToString result error: %s->%s", "test", s1)
	}
}
