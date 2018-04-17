package mapping

import (
	"time"
	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/analysis"
)

type FieldType int

const (
	StringField FieldType = 1 << iota
	NumericField
	DateField
	BooleanField
)

type FileMapping interface {
	Type() FieldType
	// The name of the field that will be stored in the index. Defaults to the property/field name.
	IndexName() string
	// Set to true to actually store the field in the index, false to not store it. Defaults to false (note, the JSON document itself is stored, and it can be retrieved from it).
	Store() bool
	Index() bool
	Boost() float64
	NullValue() interface{}
	IncludeInAll() bool
}

type StringFieldMapping struct {

}

type NumericFieldMapping struct {

}

type DateFieldMapping struct {

}

type BooleanFieldMapping struct {

}

type FieldMapping struct {
	Name       string              `json:"name,omitempty"`
	// field ID
	Id         uint32              `json:"id",omitempty`
	Type       string              `json:"type,omitempty"`

	// For analyzed string fields, use the analyzer attribute to specify which analyzer to apply both
	// at search time and at index time. By default, Baud uses the standard analyzer,
	// but you can change this by specifying one of the built-in analyzers, such as whitespace, simple, or english
	Analyzer string `json:"analyzer,omitempty"`

	// The normalizer property of keyword fields is similar to analyzer except that it guarantees that the analysis
	// chain produces a single token.
	// The normalizer is applied prior to indexing the keyword, as well as at search-time when the keyword field
	// is searched via a query parser such as the match query.
	Normalizer string `json:"normalizer",omitempty`

	Boost float64

	// Coercion attempts to clean up dirty values to fit the datatype of a field. For instance:
	// 1. Strings will be coerced to numbers.
    // 2. Floating points will be truncated for integer values.
	Coerce bool `json:"coerce",omitempty`

	// The copy_to parameter allows you to create custom _all fields. In other words, the values of multiple fields can be copied into a group field,
	// which can then be queried as a single field.
	CopyTo string `json:"copy_to",omitempty`

	// Doc values are the on-disk data structure, built at document index time, which makes this data access pattern possible.
	// They store the same values as the _source but in a column-oriented fashion that is way more efficient for sorting and aggregations. Doc values are supported on almost all field types, with the notable exception of analyzed string fields.
	DocValues bool `json:"doc_values",omitempty`

	// Baud tries to index all of the fields you give it, but sometimes you want to just store the field without indexing it.
	Enabled bool

	// just for text type field
	FieldData bool `json:"fielddata", omitempty`

	// Global ordinals is a data-structure on top of doc values, that maintains an incremental numbering for each unique term in a lexicographic order. Each term has a unique number and the number of term A is lower than the number of term B.
	// Global ordinals are only supported with keyword and text fields.
	EagerGlobalOrdinals bool `json:"eager_global_ordinals", omitempty`

	// In JSON documents, dates are represented as strings. Baud uses a set of preconfigured formats to recognize and parse these strings into a long value representing milliseconds-since-the-epoch in UTC.
	Format string `json:"format", omitempty`

	// Strings longer than the ignore_above setting will not be indexed or stored.
	IgnoreAbove uint64 `json:"ignore_above", omitempty`

	// Default false
	IgnoreMalformed bool `json:"ignore_malformed", omitempty`

	// The index_options parameter controls what information is added to the inverted index, for search and highlighting purposes. It accepts the following settings:
	// docs       Only the doc number is indexed. Can answer the question Does this term exist in this field?
	// freqs      Doc number and term frequencies are indexed. Term frequencies are used to score repeated terms higher than single terms.
	// positions  Doc number, term frequencies, and term positions (or order) are indexed. Positions can be used for proximity or phrase queries.
	// offsets    Doc number, term frequencies, positions, and start and end character offsets (which map the term back to the original string) are indexed.
	//            Offsets are used by the unified highlighter to speed up highlighting.
	IndexOptions string `json:"index_options", omitempty`

	Index bool `json:"index", omitempty`

    // Norms store various normalization factors that are later used at query time in order to compute the score of a document relatively to a query.
	Norms bool `json:"norms", omitempty`

	NullValue []byte `json:"null_value", omitempty`

	// Analyzed text fields take term positions into account, in order to be able to support proximity or phrase queries. When indexing text fields with multiple values a "fake" gap is added between the values to prevent most phrase queries from matching across the values.
	// The size of this gap is configured using position_increment_gap and defaults to 100.
	PositionIncrementGap int64 `json:"position_increment_gap", omitempty`

	SearchAnalyzer string `json:"search_analyzer", omitempty`

	// Baud allows you to configure a scoring algorithm or similarity per field. The similarity setting provides a simple way of choosing a similarity algorithm other than the default BM25, such as TF/IDF.
	// The only similarities which can be used out of the box, without any further configuration are:
    //
	// BM25
	// The Okapi BM25 algorithm. The algorithm used by default in Elasticsearch and Lucene. See Pluggable Similarity Algorithms for more information.
    // classic
    // The TF/IDF algorithm which used to be the default in Elasticsearch and Lucene. See Lucene’s Practical Scoring Function for more information.
    // boolean
    // A simple boolean similarity, which is used when full-text ranking is not needed and the score should only be based on whether the query terms match or not.
	// Boolean similarity gives terms a score equal to their query boost.
	Similarity string `json:"similarity", omitempty`

	TermVector string `json:"term_vector", omitempty`

	// Store indicates whether to store field values in the index.
	Store bool `json:"store,omitempty"`
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
