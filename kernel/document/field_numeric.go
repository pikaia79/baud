package document

import (
	"fmt"

	"github.com/tiglabs/baud/kernel/analysis"
	"github.com/blevesearch/bleve/numeric"
)

const DefaultNumericProperty = StoreField | IndexField

const DefaultPrecisionStep uint = 4

var _ Field = &NumericField{}

type NumericField struct {
	name              string
	arrayPositions    []uint64
	property          Property
	value             numeric.PrefixCoded
	numPlainTextBytes uint64
}

func (n *NumericField) Name() string {
	return n.name
}

func (n *NumericField) ArrayPositions() []uint64 {
	return n.arrayPositions
}

func (n *NumericField) Property() Property {
	return n.property
}

func (n *NumericField) Analyze() (analysis.TokenFrequencies) {
	tokens := make([]*analysis.Token, 0)
	tokens = append(tokens, &analysis.Token{
		Start:    0,
		End:      len(n.value),
		Term:     n.value,
		Position: 1,
		Type:     analysis.Numeric,
	})

	original, err := n.value.Int64()
	if err == nil {

		shift := DefaultPrecisionStep
		for shift < 64 {
			shiftEncoded, err := numeric.NewPrefixCodedInt64(original, shift)
			if err != nil {
				break
			}
			token := analysis.Token{
				Start:    0,
				End:      len(shiftEncoded),
				Term:     shiftEncoded,
				Position: 1,
				Type:     analysis.Numeric,
			}
			tokens = append(tokens, &token)
			shift += DefaultPrecisionStep
		}
	}

	tokenFreqs := analysis.TokenFrequency(tokens, n.arrayPositions, n.property.IncludeTermVectors())
	return tokenFreqs
}

func (n *NumericField) Value() []byte {
	return n.value
}

func (n *NumericField) Number() (float64, error) {
	i64, err := n.value.Int64()
	if err != nil {
		return 0.0, err
	}
	return numeric.Int64ToFloat64(i64), nil
}

func (n *NumericField) String() string {
	return fmt.Sprintf("&document.NumericField{Name:%s, Property: %s, Value: %s}", n.name, n.property, n.value)
}

func NewNumericFieldFromBytes(name string, arrayPositions []uint64, value []byte) *NumericField {
	return &NumericField{
		name:              name,
		arrayPositions:    arrayPositions,
		value:             value,
		property:          DefaultNumericProperty,
		numPlainTextBytes: uint64(len(value)),
	}
}

func NewNumericField(name string, arrayPositions []uint64, number float64) *NumericField {
	return NewNumericFieldWithProperty(name, arrayPositions, number, DefaultNumericProperty)
}

func NewNumericFieldWithProperty(name string, arrayPositions []uint64, number float64, property Property) *NumericField {
	numberInt64 := numeric.Float64ToInt64(number)
	prefixCoded := numeric.MustNewPrefixCodedInt64(numberInt64, 0)
	return &NumericField{
		name:           name,
		arrayPositions: arrayPositions,
		value:          prefixCoded,
		property:       property,
	}
}
