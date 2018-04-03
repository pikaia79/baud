package document

import (
	"fmt"
)

type BooleanField struct {
	name              string
	value             []byte
	numPlainTextBytes uint64
}

func (b *BooleanField) Name() string {
	return b.name
}

func (b *BooleanField) Value() []byte {
	return b.value
}

func (b *BooleanField) Boolean() (bool, error) {
	if len(b.value) == 1 {
		return b.value[0] == 'T', nil
	}
	return false, fmt.Errorf("boolean field has %d bytes", len(b.value))
}

func (b *BooleanField) String() string {
	return fmt.Sprintf("&document.BooleanField{Name:%s, Value: %s}", b.name, b.value)
}

func (b *BooleanField) NumPlainTextBytes() uint64 {
	return b.numPlainTextBytes
}

func NewBooleanFieldFromBytes(name string, value []byte) *BooleanField {
	return &BooleanField{
		name:              name,
		value:             value,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewBooleanField(name string, b bool) *BooleanField {
	numPlainTextBytes := 5
	v := []byte("F")
	if b {
		numPlainTextBytes = 4
		v = []byte("T")
	}
	return &BooleanField{
		name:              name,
		value:             v,
		numPlainTextBytes: uint64(numPlainTextBytes),
	}
}
