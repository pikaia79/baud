package document

import (
	"fmt"

	"github.com/tiglabs/baudengine/kernel/analysis"
)

var _ Field = &BooleanField{}

type BooleanField struct {
	name              string
	property          Property
	value             []byte
}

func (b *BooleanField) Name() string {
	return b.name
}

func (b *BooleanField) Property() Property {
	return b.property
}

func (b *BooleanField) Analyze() (analysis.TokenFrequencies) {
	tokens := make([]*analysis.Token, 0)
	tokens = append(tokens, &analysis.Token{
		Start:    0,
		End:      len(b.value),
		Term:     b.value,
		Position: 1,
		Type:     analysis.Boolean,
	})

	tokenFreqs := analysis.TokenFrequency(tokens, b.property.IncludeTermVectors())
	return tokenFreqs
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
	return fmt.Sprintf("&document.BooleanField{Name:%s, Property: %s, Value: %s}", b.name, b.property, b.value)
}

func NewBooleanFieldByBytes(name string, value []byte, property Property) *BooleanField {
	return &BooleanField{
		name:              name,
		value:             value,
		property:          property,
	}
}

func NewBooleanField(name string, b bool, property Property) *BooleanField {
	return NewBooleanFieldWithProperty(name, b, property)
}

func NewBooleanFieldWithProperty(name string, b bool, property Property) *BooleanField {
	v := []byte("F")
	if b {
		v = []byte("T")
	}
	return &BooleanField{
		name:              name,
		value:             v,
		property:          property,
	}
}
