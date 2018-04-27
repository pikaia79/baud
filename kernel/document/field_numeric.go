package document

import (
	"fmt"

	"github.com/tiglabs/baudengine/kernel/analysis"
	"github.com/tiglabs/baudengine/util"
)

const DefaultPrecisionStep uint = 4

var _ Field = &NumericField{}

type NumericField struct {
	name              string
	property          Property
	value             util.Value
}

func (n *NumericField) Name() string {
	return n.name
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
			shiftEncoded, err := util.NewPrefixCodedInt64(original, shift)
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

	tokenFreqs := analysis.TokenFrequency(tokens, n.property.IncludeTermVectors())
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
	return util.Int64ToFloat64(i64), nil
}

func (n *NumericField) String() string {
	return fmt.Sprintf("&document.NumericField{Name:%s, Property: %s, Value: %s}", n.name, n.property, n.value)
}

func NewNumericFieldFromBytes(name string, value []byte, property Property) *NumericField {
	return &NumericField{
		name:              name,
		value:             value,
		property:          property,
	}
}

func NewNumericField(name string, number float64, property Property) *NumericField {
	return NewNumericFieldWithProperty(name, number, property)
}

func NewNumericFieldWithProperty(name string, number float64, property Property) *NumericField {
	numberInt64 := util.Float64ToInt64(number)
	prefixCoded := util.PrefixCodedInt64(numberInt64, 0)
	return &NumericField{
		name:           name,
		value:          prefixCoded,
		property:       property,
	}
}
