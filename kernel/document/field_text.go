package document

import (
	"fmt"

	"github.com/tiglabs/baud/kernel/analysis"
)

const DefaultTextProperty = IndexField | DocValues

var _ Field = &TextField{}

type TextField struct {
	name              string
	arrayPositions    []uint64
	property          Property
	analyzer          analysis.Analyzer
	value             []byte
}

func (t *TextField) Name() string {
	return t.name
}

func (t *TextField) ArrayPositions() []uint64 {
	return t.arrayPositions
}

func (t *TextField) Property() Property {
	return t.property
}

func (t *TextField) Analyze() (analysis.TokenFrequencies) {
	var tokens []*analysis.Token
	if t.analyzer != nil {
		bytesToAnalyze := t.Value()
		if t.property.IsStored() {
			// need to copy
			bytesCopied := make([]byte, len(bytesToAnalyze))
			copy(bytesCopied, bytesToAnalyze)
			bytesToAnalyze = bytesCopied
		}
		tokens = t.analyzer.Analyze(bytesToAnalyze)
	} else {
		tokens = []*analysis.Token{
			&analysis.Token{
				Start:    0,
				End:      len(t.value),
				Term:     t.value,
				Position: 1,
				Type:     analysis.Text,
			},
		}
	}
	tokenFreqs := analysis.TokenFrequency(tokens, t.arrayPositions, t.property.IncludeTermVectors())
	return tokenFreqs
}

func (t *TextField) Value() []byte {
	return t.value
}

func (t *TextField) String() string {
	return fmt.Sprintf("&document.TextField{Name:%s, Property: %s, Analyzer: %v, Value: %s, ArrayPositions: %v}",
		t.name, t.property, t.analyzer, t.value, t.arrayPositions)
}

func NewTextField(name string, arrayPositions []uint64, value []byte) *TextField {
	return NewTextFieldWithIndexingOptions(name, arrayPositions, value, DefaultTextProperty)
}

func NewTextFieldWithIndexingOptions(name string, arrayPositions []uint64, value []byte, property Property) *TextField {
	return &TextField{
		name:              name,
		arrayPositions:    arrayPositions,
		property:          property,
		value:             value,
	}
}

func NewTextFieldWithAnalyzer(name string, arrayPositions []uint64, value []byte, analyzer analysis.Analyzer) *TextField {
	return &TextField{
		name:              name,
		arrayPositions:    arrayPositions,
		property:          DefaultTextProperty,
		analyzer:          analyzer,
		value:             value,
	}
}

func NewTextFieldCustom(name string, arrayPositions []uint64, value []byte, property Property, analyzer analysis.Analyzer) *TextField {
	return &TextField{
		name:              name,
		arrayPositions:    arrayPositions,
		property:          property,
		analyzer:          analyzer,
		value:             value,
	}
}
