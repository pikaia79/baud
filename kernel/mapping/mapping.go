package mapping

import (
	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/analysis"
)

type IndexMapping interface {
	MapDocument(doc *document.Document, data []byte) error
	AnalyzerNamed(name string) analysis.Analyzer
	DateTimeParserNamed(name string) analysis.DateTimeParser
}