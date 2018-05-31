package document

import (
	"testing"
	"github.com/tiglabs/baudengine/kernel/analysis"
)

type EmptyField struct {
	name    string
}

func(f *EmptyField) Name() string {return f.name}
// Property for different store driver and encode type
func(f *EmptyField) Property() Property {
	return IndexField
}
func(f *EmptyField) Analyze() analysis.TokenFrequencies {
	return nil
}
func(f *EmptyField) Value() []byte {
	return nil
}

// Return next to field in array field
// Return nil if the field is the end of array field
func(f *EmptyField) Next() Field {return nil}

func TestDocumentAddField(t *testing.T) {
	doc := NewDocument("1")
	doc.AddField(&EmptyField{name: "f1"})
	doc.AddField(&EmptyField{name: "f1"})
	doc.AddField(&EmptyField{name: "f2"})
	fs := doc.FindFields("f1")
	if len(fs) != 2 {
		t.Fatal("add field failed")
	}
}
