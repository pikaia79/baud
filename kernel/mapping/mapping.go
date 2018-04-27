package mapping

import (
	"github.com/tiglabs/baudengine/kernel/document"
	"github.com/tiglabs/baudengine/kernel/analysis"
)

type IndexMapping interface {
	// rebuild _all field
	RebuildAllField(doc *document.Document) error
	MergeDocument(doc *document.Document, source []byte) error
	MapDocument(doc *document.Document, data []byte) error
	AnalyzerNamed(name string) analysis.Analyzer
	DateTimeParserNamed(name string) analysis.DateTimeParser
}