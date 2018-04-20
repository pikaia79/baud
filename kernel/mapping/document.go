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
		err := dm.Mapping.ParseField(doc, path, context)
		if err != nil {
			return err
		}
		// TODO _all field
		allFieldMapping := dm.Mapping.FindProperty("_all")
		if allFieldMapping != nil && allFieldMapping.Enabled() {
			field := document.NewCompositeFieldWithProperty("_all", context.excludedFromAll, allFieldMapping.(*TextFieldMapping).Property())
			context.doc.AddField(field)
		}
		// TODO _source field
	}
	return nil
}

type parseContext struct {
	doc             *document.Document
	im              IndexMapping
	dm              *DocumentMapping
	excludedFromAll []string
}