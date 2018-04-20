package document

import (
	"fmt"

	"github.com/tiglabs/baud/kernel/analysis"
)

var _ Field = &TextField{}

type TextField struct {
	name              string
	property          Property
	analyzer          analysis.Analyzer
	value             []byte
}

func (t *TextField) Name() string {
	return t.name
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
	tokenFreqs := analysis.TokenFrequency(tokens, t.property.IncludeTermVectors())
	return tokenFreqs
}

func (t *TextField) Value() []byte {
	return t.value
}

func (t *TextField) String() string {
	return fmt.Sprintf("&document.TextField{Name:%s, Property: %s, Analyzer: %v, Value: %s}",
		t.name, t.property, t.analyzer, t.value)
}

func NewTextField(name string, value []byte, property Property) *TextField {
	return NewTextFieldWithIndexingOptions(name, value, property)
}

func NewTextFieldWithIndexingOptions(name string, value []byte, property Property) *TextField {
	return &TextField{
		name:              name,
		property:          property,
		value:             value,
	}
}

func NewTextFieldWithAnalyzer(name string, value []byte, property Property, analyzer analysis.Analyzer) *TextField {
	return &TextField{
		name:              name,
		property:          property,
		analyzer:          analyzer,
		value:             value,
	}
}

func NewTextFieldCustom(name string, value []byte, property Property, analyzer analysis.Analyzer) *TextField {
	return &TextField{
		name:              name,
		property:          property,
		analyzer:          analyzer,
		value:             value,
	}
}
