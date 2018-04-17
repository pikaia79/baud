package mapping

import "github.com/tiglabs/baud/kernel/document"

type Mapping interface {
	MapDocument(doc *document.Document, data []byte) error
}