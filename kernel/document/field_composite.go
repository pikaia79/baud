package document

import "github.com/tiglabs/baud/kernel/analysis"

const space = byte(' ')

var _ Field = &CompositeField{}

//
type CompositeField struct {
	name                 string
	excludedFields       map[string]bool
	property             Property
	value                []byte
	compositeFrequencies analysis.TokenFrequencies
}

func NewCompositeField(name string, exclude []string, property Property) *CompositeField {
	return NewCompositeFieldWithProperty(name, exclude, property)
}

func NewCompositeFieldWithProperty(name string, exclude []string, property Property) *CompositeField {
	rv := &CompositeField{
		name:                 name,
		property:             property,
		excludedFields:       make(map[string]bool, len(exclude)),
		compositeFrequencies: make(analysis.TokenFrequencies),
	}

	for _, e := range exclude {
		rv.excludedFields[e] = true
	}

	return rv
}

func (c *CompositeField) Name() string {
	return c.name
}

func (c *CompositeField) Property() Property {
	return c.property
}

func (c *CompositeField) Analyze() (analysis.TokenFrequencies) {
	return c.compositeFrequencies
}

func (c *CompositeField) Value() []byte {
	return c.value
}

func (c *CompositeField) includeField(field string) bool {
	if _, excluded := c.excludedFields[field]; excluded {
		return false
	}
	return true
}

//
func (c *CompositeField) MergeAll(field string, value []byte, freq analysis.TokenFrequencies) {
	if c.includeField(field) {
		if c.property.IsIndexed() {
			c.compositeFrequencies.MergeAll(field, freq)
		}
		if c.property.IsStored() && len(value) > 0 {
			c.value = append(c.value, space)
			c.value = append(c.value, value...)
		}
	}
}
