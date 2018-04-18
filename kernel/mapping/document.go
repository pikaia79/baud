package mapping

import (
	"github.com/tiglabs/baud/kernel/document"
)

type DocumentMapping struct {
	Mapping *ObjectFieldMapping            `json:"mappings"`
	StructTagKey string                    `json:"-"`
}

func NewDocumentMapping(mapping *ObjectFieldMapping) *DocumentMapping {
	doc := &DocumentMapping{StructTagKey: "json", Mapping: mapping}
	return doc
}

func (dm *DocumentMapping) parseDocument(doc interface{}, context *parseContext) error {
	if dm.Mapping != nil {
		var path []string
		return dm.Mapping.ParseField(doc, path, context)
		// TODO _all field
	}
	return nil
}

type parseContext struct {
	doc             *document.Document
	im              IndexMapping
	dm              *DocumentMapping
	excludedFromAll []string
}