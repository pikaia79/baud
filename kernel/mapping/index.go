package mapping

import "github.com/tiglabs/baud/kernel/document"

type IndexMapping struct {
	docMapping   *DocumentMapping
}

func (im *IndexMapping) MapDocument(doc *document.Document, data interface{}) error {
	if im.docMapping == nil {
		return error.Error("invalid document to do mapping")
	}
	if im.docMapping.Enabled {

	}
}


type mappingContext struct {
	doc             *document.Document
	im              *IndexMappingImpl
	dm              *DocumentMapping
	excludedFromAll []string
}

func (im *IndexMappingImpl) newWalkContext(doc *document.Document, dm *DocumentMapping) *walkContext {
	return &walkContext{
		doc:             doc,
		im:              im,
		dm:              dm,
		excludedFromAll: []string{"_id"},
	}
}
