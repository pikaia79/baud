package document

import (
	"fmt"

	"github.com/tiglabs/baud/kernel/analysis"
)

const DefaultBooleanProperty = StoreField | IndexField

var _ Field = &BooleanField{}

type BooleanField struct {
	name              string
	arrayPositions    []uint64
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

	tokenFreqs := analysis.TokenFrequency(tokens, b.arrayPositions, b.property.IncludeTermVectors())
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

func NewBooleanFieldByBytes(name string, arrayPositions []uint64, value []byte) *BooleanField {
	return &BooleanField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		property:          DefaultBooleanProperty,
	}
}

func NewBooleanField(name string, arrayPositions []uint64, b bool) *BooleanField {
	return NewBooleanFieldWithProperty(name, arrayPositions, b, DefaultBooleanProperty)
}

func NewBooleanFieldWithProperty(name string, arrayPositions []uint64, b bool, property Property) *BooleanField {
	v := []byte("F")
	if b {
		v = []byte("T")
	}
	return &BooleanField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             v,
		property:          property,
	}
}
