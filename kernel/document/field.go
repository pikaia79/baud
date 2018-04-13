package document

import "github.com/tiglabs/baud/kernel/analysis"

type Field interface {
	// Name returns the path of the field from the root DocumentMapping.
	// A root field path is "field", a sub document field is "parent.field".
	Name() string
	// Property for different store driver and encode type
	Property() Property
	Analyze() analysis.TokenFrequencies
	Value() []byte
}
