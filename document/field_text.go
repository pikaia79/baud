package document

import (
	"fmt"
)

type TextField struct {
	name              string
	value             []byte
	numPlainTextBytes uint64
}

func (t *TextField) Name() string {
	return t.name
}

func (t *TextField) Value() []byte {
	return t.value
}

func (t *TextField) String() string {
	return fmt.Sprintf("&document.TextField{Name:%s, Value: %s}", t.name, t.value)
}

func (t *TextField) NumPlainTextBytes() uint64 {
	return t.numPlainTextBytes
}

func NewTextField(name string, value []byte) *TextField {
	return &TextField{
		name:              name,
		value:             value,
		numPlainTextBytes: uint64(len(value)),
	}
}

