package mapping

import (
	"time"
	"github.com/blevesearch/bleve/geo"
	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/analysis"
)

type FieldMapping struct {
	Name       string              `json:"name,omitempty"`
	Type       string              `json:"type,omitempty"`

	// For analyzed string fields, use the analyzer attribute to specify which analyzer to apply both
	// at search time and at index time. By default, Elasticsearch uses the standard analyzer,
	// but you can change this by specifying one of the built-in analyzers, such as whitespace, simple, or english
	Analyzer string `json:"analyzer,omitempty"`

	// The index attribute controls how the string will be indexed. It can contain one of three values:
	// analyzed
	// First analyze the string and then index it. In other words, index this field as full text.
	// not_analyzed
	// Index this field, so it is searchable, but index the value exactly as specified. Do not analyze it.
	// no
	// Don’t index this field at all. This field will not be searchable.
	// The default value of index for a string field is analyzed.
	Index string `json:"index,omitempty"`

	// Store indicates whether to store field values in the index.
	Store bool `json:"store,omitempty"`

	// IncludeTermVectors, if true, makes terms occurrences to be recorded for
	// this field. It includes the term position within the terms sequence and
	// the term offsets in the source document field. Term vectors are required
	// to perform phrase queries or terms highlighting in source documents.
	IncludeTermVectors bool   `json:"include_term_vectors,omitempty"`
	// IncludeInAll, default true, for _all to make index
	IncludeInAll       bool   `json:"include_in_all,omitempty"`
	DateFormat         string `json:"date_format,omitempty"`

	// DocValues, if true makes the index uninverting possible for this field
	// It is useful for faceting and sorting queries.
	DocValues bool `json:"docvalues,omitempty"`
}


// Options returns the indexing options for this field.
func (fm *FieldMapping) Property() document.Property {
	var p document.Property
	if fm.Store {
		p |= document.StoreField
	}
	if fm.Index != "no" {
		p |= document.IndexField
	}
	if fm.IncludeTermVectors {
		p |= document.TermVectors
	}
	if fm.DocValues {
		p |= document.DocValues
	}
	return p
}

func (fm *FieldMapping) processString(propertyValueString string, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	options := fm.Property()
	if fm.Type == "text" {
		analyzer := fm.analyzerForField(path, context)
		field := document.NewTextFieldCustom(fieldName, indexes, []byte(propertyValueString), options, analyzer)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	} else if fm.Type == "datetime" {
		dateTimeFormat := context.im.DefaultDateTimeParser
		if fm.DateFormat != "" {
			dateTimeFormat = fm.DateFormat
		}
		dateTimeParser := context.im.DateTimeParserNamed(dateTimeFormat)
		if dateTimeParser != nil {
			parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
			if err == nil {
				fm.processTime(parsedDateTime, pathString, path, indexes, context)
			}
		}
	}
}

func (fm *FieldMapping) processFloat64(propertyValFloat float64, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "number" {
		options := fm.Options()
		field := document.NewNumericFieldWithIndexingOptions(fieldName, indexes, propertyValFloat, options)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) processTime(propertyValueTime time.Time, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "datetime" {
		options := fm.Options()
		field, err := document.NewDateTimeFieldWithIndexingOptions(fieldName, indexes, propertyValueTime, options)
		if err == nil {
			context.doc.AddField(field)
		} else {
			logger.Printf("could not build date %v", err)
		}

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) processBoolean(propertyValueBool bool, pathString string, path []string, indexes []uint64, context *walkContext) {
	fieldName := getFieldName(pathString, path, fm)
	if fm.Type == "boolean" {
		options := fm.Options()
		field := document.NewBooleanFieldWithIndexingOptions(fieldName, indexes, propertyValueBool, options)
		context.doc.AddField(field)

		if !fm.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	}
}

func (fm *FieldMapping) analyzerForField(path []string, context *walkContext) analysis.Analyzer {
	analyzerName := fm.Analyzer
	if analyzerName == "" {
		analyzerName = context.dm.defaultAnalyzerName(path)
		if analyzerName == "" {
			analyzerName = context.im.DefaultAnalyzer
		}
	}
	return context.im.AnalyzerNamed(analyzerName)
}

func getFieldName(pathString string, path []string, fieldMapping *FieldMapping) string {
	fieldName := pathString
	if fieldMapping.Name != "" {
		parentName := ""
		if len(path) > 1 {
			parentName = encodePath(path[:len(path)-1]) + pathSeparator
		}
		fieldName = parentName + fieldMapping.Name
	}
	return fieldName
}
