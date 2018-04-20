package mapping

import (
	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/analysis"
)

type IndexMapping interface {
	// rebuild _all field
	RebuildAllField(doc *document.Document) error
	MergeDocument(doc *document.Document, source []byte) error
	MapDocument(doc *document.Document, data []byte) error
	AnalyzerNamed(name string) analysis.Analyzer
	DateTimeParserNamed(name string) analysis.DateTimeParser
}