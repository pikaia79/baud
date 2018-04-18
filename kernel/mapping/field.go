package mapping

import (
	"reflect"
	"errors"
	"strings"
	"strconv"
	"fmt"

	"github.com/tiglabs/baud/kernel/document"
	"time"
)

type FieldMapping interface {
	Name() string
	Type() string
	ID()   uint64
	Store() bool
	Index() bool
	Enabled() bool
	ParseField(data interface{}, path []string, context *parseContext) error
}

type EmptyFieldMapping struct {}
func(e *EmptyFieldMapping) Name() string {return ""}
func(e *EmptyFieldMapping) Type() string {return ""}
func(e *EmptyFieldMapping) ID()   uint64 {return 0}
func(e *EmptyFieldMapping) Store() bool {return false}
func(e *EmptyFieldMapping) Index() bool {return false}
func(e *EmptyFieldMapping) Enabled() bool {return false}
func(e *EmptyFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	return nil
}

type DynamicFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	Dynamic bool                   `json:"dynamic,omitempty"`
	IncludeInAll bool              `json:"include_in_all,omitempty"`
	Properties map[string]FieldMapping `json:"properties,omitempty"`
}

// type only can be object or nested
func NewDynamicFieldMapping(name, type_ string, id uint64) *DynamicFieldMapping {
	return &DynamicFieldMapping{
		Name_: name,
		Id: id,
		Type_: type_,
		Enabled_: true,
		Dynamic: true,
	}
}

func(f *DynamicFieldMapping) Name() string {return f.Name_}
func(f *DynamicFieldMapping) Type() string {return f.Type_}
func(f *DynamicFieldMapping) ID()   uint64 {return f.Id}
func(f *DynamicFieldMapping) Store() bool {return false}
func(f *DynamicFieldMapping) Index() bool {return false}
func(f *DynamicFieldMapping) Enabled() bool {return false}
func(f *DynamicFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	// todo
	return nil
}

type ObjectFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	Dynamic bool                   `json:"dynamic,omitempty"`
	IncludeInAll bool              `json:"include_in_all,omitempty"`
	Properties map[string]FieldMapping `json:"properties,omitempty"`
}

func NewObjectFieldMapping(name string, id uint64) *ObjectFieldMapping {
	return &ObjectFieldMapping {
		Name_: name,
		Id: id,
		Type_: "object",
		Enabled_: true,
		Dynamic: false,
		IncludeInAll: true,
	}
}

func(f *ObjectFieldMapping) Name() string {return f.Name_}
func(f *ObjectFieldMapping) Type() string {return "object"}
func(f *ObjectFieldMapping) ID()   uint64 {return 0}
func(f *ObjectFieldMapping) Store() bool {return false}
func(f *ObjectFieldMapping) Index() bool {return true}
func(f *ObjectFieldMapping) Enabled() bool {return true}
func(f *ObjectFieldMapping) AddFileMapping(field FieldMapping) {
	if f.Properties == nil {
		f.Properties = make( map[string]FieldMapping)
	}
	f.Properties[field.Name()] = field
}

func (f *ObjectFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	if !f.Enabled() {
		return nil
	}
	// not support dynamic now
	if f.Dynamic {
		// todo
		return nil
	}
	val := reflect.ValueOf(data)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		if typ.Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				// get field
				if f.Properties == nil {
					return errors.New("Fields that can not be identified")
				}
				field, ok := f.Properties[key.String()]
				if !ok {
					return fmt.Errorf("Fields %s that can not be identified", key.String())
				}
				
				fieldName := key.String()
				fieldVal := val.MapIndex(key).Interface()
				err := field.ParseField(fieldVal, append(path, f.Name(), fieldName), context)
				if err != nil {
					return err
				}
			}
		}
	default:
		return errors.New("Fields that can not be identified")
	}
	return nil
}

type NestedFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	Dynamic bool                   `json:"dynamic,omitempty"`
	IncludeInAll bool              `json:"include_in_all,omitempty"`
	Properties map[string]FieldMapping `json:"properties,omitempty"`
}

func NewNestedFieldMapping(name string, id uint64) *NestedFieldMapping {
	return &NestedFieldMapping{
		Name_: name,
		Id: id,
		Type_: "nested",
		Enabled_: true,
		Dynamic: false,
		IncludeInAll: true,
	}
}

func(f *NestedFieldMapping) Name() string {return f.Name_}
func(f *NestedFieldMapping) Type() string {return "nested"}
func(f *NestedFieldMapping) ID()   uint64 {return 0}
func(f *NestedFieldMapping) Store() bool {return false}
func(f *NestedFieldMapping) Index() bool {return true}
func(f *NestedFieldMapping) Enabled() bool {return true}
func(f *NestedFieldMapping) AddFileMapping(field FieldMapping) {
	if f.Properties == nil {
		f.Properties = make( map[string]FieldMapping)
	}
	f.Properties[field.Name()] = field
}

func(f *NestedFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	if !f.Enabled() {
		return nil
	}
	// todo
	return nil
}

type FieldDataFrequencyFilter struct {
	Min    float64       `json:"min,omitempty"`
	Max    float64       `json:"max,omitempty"`
	MinSegmentSize int64 `json:"min_segment_size,omitempty"`
}

type TextFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	Analyzer_ string               `json:"analyzer,omitempty"`
	Boost float64                  `json:"boost,omitempty"`
	IncludeInAll bool              `json:"include_in_all,omitempty"`
	FieldData bool                 `json:"fielddata,omitempty"`
	// Expert settings which allow to decide which values to load in memory when fielddata is enabled. By default all values are loaded.
	FieldDataFrequencyFilter *FieldDataFrequencyFilter `json:"fielddata_frequency_filter,omitempty"`
	Fields map[string]FieldMapping `json:"fields,omitempty"`
	Index_ bool                    `json:"index,omitempty"`
	IndexOptions string            `json:"index_options,omitempty"`
	Norms bool                     `json:"norms,omitempty"`
	PositionIncrementGap int64     `json:"position_increment_gap,omitempty"`
	Store_ bool                    `json:"store,omitempty"`
	SearchAnalyzer string          `json:"search_analyzer,omitempty"`
	SearchQuoteAnalyzer string     `json:"search_quote_analyzer,omitempty"`
	Similarity string              `json:"similarity,omitempty"`
	TermVector string              `json:"term_vector,omitempty"`
}

func NewTextFieldMapping(name string, id uint64) *TextFieldMapping {
	return &TextFieldMapping{
		Name_:  name,
		Id: id,
		Type_:  "text",
		Enabled_: true,
		Analyzer_: "standard",
		Boost: 1.0,
		IncludeInAll: true,
		FieldData: false,
		Index_: true,
		IndexOptions: "positions",
		Norms: true,
		PositionIncrementGap: 100,
		Store_: false,
		SearchAnalyzer: "standard",
		SearchQuoteAnalyzer: "standard",
		Similarity: "BM25",
		TermVector: "no",
	}
}

func (f *TextFieldMapping) Name() string {
	return f.Name_
}

func (f *TextFieldMapping) Type() string {
	return f.Type_
}

func (f *TextFieldMapping) ID() uint64 {
	return f.Id
}

func (f *TextFieldMapping) Store() bool {
	return f.Store_
}

func (f *TextFieldMapping) Index() bool {
	return f.Index_
}

func (f *TextFieldMapping) Enabled() bool {
	return f.Enabled_
}

func (f *TextFieldMapping) Property() document.Property {
	var p document.Property
	if f.Store() {
		p |= document.StoreField
	}
	if f.Index() {
		p |= document.IndexField
	}
	if f.TermVector != "no" {
		p |= document.TermVectors
	}
	return p
}

func (f *TextFieldMapping) AddField(field FieldMapping) {
	// fixme check field type
	if f.Fields == nil {
		f.Fields = make(map[string]FieldMapping)
	}
	f.Fields[field.Name()] = field
}

func (f *TextFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	if !f.Enabled() {
		return nil
	}
	val := reflect.ValueOf(data)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.IsValid() && ptrElem.CanInterface() {
			f.ParseField(ptrElem.Interface(), path, context)
		}
	case reflect.String:
		propertyValueString := val.String()
		analyzer := context.im.AnalyzerNamed(f.Analyzer_)
		fieldName := getFieldName(path, f)
		field := document.NewTextFieldCustom(getFieldName(path, f), []byte(propertyValueString), f.Property(), analyzer)
		context.doc.AddField(field)

		if !f.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
		for name, fieldMapping := range f.Fields {
			err := fieldMapping.ParseField(propertyValueString, append(path, fieldName, name), context)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid field type")
	}
	return nil
}

type KeywordFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	Boost float64                  `json:"boost,omitempty"`
	DocValues bool                 `json:"doc_values,omitempty"`
	IncludeInAll bool              `json:"include_in_all,omitempty"`
	Fields map[string]FieldMapping `json:"fields,omitempty"`
	IgnoreAbove uint64             `json:"ignore_above,omitempty"`
	Index_ bool                    `json:"index,omitempty"`
	IndexOptions string            `json:"index_options,omitempty"`
	Norms bool                     `json:"norms,omitempty"`
	NullValue string               `json:"null_value,omitempty"`
	Store_ bool                    `json:"store,omitempty"`
	Similarity string              `json:"similarity,omitempty"`
	Normalizer string              `json:"normalizer,omitempty"`
}

func NewKeywordFieldMapping(name string, id uint64) *KeywordFieldMapping {
	return &KeywordFieldMapping{
		Name_:  name,
		Id: id,
		Type_:  "keyword",
		Enabled_: true,
		Boost: 1.0,
		IncludeInAll: true,
		DocValues: true,
		IgnoreAbove: 2147483647,
		Index_: true,
		IndexOptions: "docs",
		Norms: false,
		Store_: false,
		Similarity: "BM25",
	}
}

func(f *KeywordFieldMapping) Name() string {return f.Name_}
func(f *KeywordFieldMapping) Type() string {return f.Type_}
func(f *KeywordFieldMapping) ID()   uint64 {return f.Id}
func(f *KeywordFieldMapping) Store() bool {return f.Store_}
func(f *KeywordFieldMapping) Index() bool {return f.Index_}
func(f *KeywordFieldMapping) Enabled() bool {return f.Enabled_}
func(f *KeywordFieldMapping) Property() document.Property {
	var p document.Property
	if f.Store() {
		p |= document.StoreField
	}
	if f.Index() {
		p |= document.IndexField
	}
	if f.DocValues {
		p |= document.DocValues
	}
	return p
}
func (f *KeywordFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	if !f.Enabled() {
		return nil
	}
	val := reflect.ValueOf(data)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.IsValid() && ptrElem.CanInterface() {
			f.ParseField(ptrElem.Interface(), path, context)
		}
	case reflect.String:
		propertyValueString := val.String()
		fieldName := getFieldName(path, f)
		field := document.NewTextFieldCustom(getFieldName(path, f), []byte(propertyValueString), f.Property(), nil)
		context.doc.AddField(field)

		if !f.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
		for name, fieldMapping := range f.Fields {
			err := fieldMapping.ParseField(propertyValueString, append(path, name), context)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("invalid field type")
	}
	return nil
}

type NumericFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	IncludeInAll bool              `json:"include_in_all,omitempty"`
	Coerce bool                    `json:"coerce,omitempty"`
	Boost float64                  `json:"boost,omitempty"`
	DocValues bool                 `json:"doc_values,omitempty"`
	IgnoreMalformed bool           `json:"ignore_malformed,omitempty"`
	Index_ bool                     `json:"index,omitempty"`
	NullValue float64              `json:"null_value,omitempty"`
	Store_ bool                    `json:"store,omitempty"`
}

func NewNumericFieldMapping(name string, typ string, id uint64) *NumericFieldMapping{
	return &NumericFieldMapping{
		Name_: name,
		Id: id,
		Type_: typ,
		Enabled_: true,
		IncludeInAll: true,
		Coerce: true,
		Boost: 1.0,
		DocValues: true,
		IgnoreMalformed: false,
		Index_: true,
		Store_: false,
	}
}

func(f *NumericFieldMapping) Name() string {return f.Name_}
func(f *NumericFieldMapping) Type() string {return f.Type_}
func(f *NumericFieldMapping) ID()   uint64 {return f.Id}
func(f *NumericFieldMapping) Store() bool {return f.Store_}
func(f *NumericFieldMapping) Index() bool {return f.Index_}
func(f *NumericFieldMapping) Enabled() bool {return f.Enabled_}
func(f *NumericFieldMapping) Property() document.Property {
	var p document.Property
	if f.Store() {
		p |= document.StoreField
	}
	if f.Index() {
		p |= document.IndexField
	}
	if f.DocValues {
		p |= document.DocValues
	}
	return p
}
func(f *NumericFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	if !f.Enabled() {
		return nil
	}
	propertyValue := reflect.ValueOf(data)
	if !propertyValue.IsValid() {
		// cannot do anything with the zero value
		return errors.New("field value invalid")
	}
	var propertyValFloat float64
	var err error

	propertyType := propertyValue.Type()
	switch propertyType.Kind() {
	case reflect.String:
		if f.Coerce {
			propertyValueString := propertyValue.String()
			switch f.Type() {
			case "long", "integer", "short", "byte":
				var _val int64
				_val, err = strconv.ParseInt(propertyValueString, 10, 64)
				propertyValFloat = float64(_val)
			case "double", "float", "half_float", "scaled_float":
				propertyValFloat, err = strconv.ParseFloat(propertyValueString, 64)
			default:
				return fmt.Errorf("invalid field type %s", f.Type())
			}
			// todo ignore_malformed
			if err != nil {
				return err
			}
		} else {
			return errors.New("invalid field type")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		propertyValFloat = float64(propertyValue.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		propertyValFloat = float64(propertyValue.Uint())
	case reflect.Float64, reflect.Float32:
		propertyValFloat = propertyValue.Float()
	default:
		return fmt.Errorf("invalid field type %s", propertyType.Kind().String())
	}
	fieldName := getFieldName(path, f)
	field := document.NewNumericFieldWithProperty(fieldName, propertyValFloat, f.Property())
	context.doc.AddField(field)
	if !f.IncludeInAll {
		context.excludedFromAll = append(context.excludedFromAll, fieldName)
	}
	return nil
}

type DateFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	IncludeInAll bool              `json:"include_in_all,omitempty"`
	Boost float64                  `json:"boost,omitempty"`
	DocValues bool                 `json:"doc_values,omitempty"`
	Format string                  `json:"format,omitempty"`
	// The locale to use when parsing dates since months do not have the same names and/or abbreviations in all languages.
	// The default is the ROOT locale,
	// TODO locale

	IgnoreMalformed bool           `json:"ignore_malformed,omitempty"`
	Index_ bool                    `json:"index,omitempty"`
	Store_ bool                    `json:"store,omitempty"`
}

func NewDateFieldMapping(name string, id uint64) *DateFieldMapping {
	return &DateFieldMapping{
		Name_: name,
		Id: id,
		Type_: "date",
		Enabled_: true,
		IncludeInAll: true,
		Boost: 1.0,
		DocValues: true,
		Format: "strict_date_optional_time||epoch_millis",
		IgnoreMalformed: false,
		Index_: true,
		Store_: false,
	}
}

func(f *DateFieldMapping) Name() string {return f.Name_}
func(f *DateFieldMapping) Type() string {return f.Type_}
func(f *DateFieldMapping) ID()   uint64 {return f.Id}
func(f *DateFieldMapping) Store() bool {return f.Store_}
func(f *DateFieldMapping) Index() bool {return f.Index_}
func(f *DateFieldMapping) Enabled() bool {return f.Enabled_}
func(f *DateFieldMapping) Property() document.Property {
	var p document.Property
	if f.Store() {
		p |= document.StoreField
	}
	if f.Index() {
		p |= document.IndexField
	}
	if f.DocValues {
		p |= document.DocValues
	}
	return p
}

func(f *DateFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	if !f.Enabled() {
		return nil
	}
	propertyValue := reflect.ValueOf(data)
	if !propertyValue.IsValid() {
		// cannot do anything with the zero value
		return errors.New("field value invalid")
	}
	propertyType := propertyValue.Type()
	switch propertyType.Kind() {
	case reflect.String:
		propertyValueString := propertyValue.String()
		formats := strings.Split(f.Format, "||")
		var parsedDateTime time.Time
		var err error
		for _, format := range formats {
			dateTimeParser := context.im.DateTimeParserNamed(format)
			if dateTimeParser != nil {
				parsedDateTime, err = dateTimeParser.ParseDateTime(propertyValueString)
				// parse time ok
				if err == nil {
					break
				}
			}
		}
		if err != nil {
			// todo ignore_malformed
			return err
		}
		fieldName := getFieldName(path, f)
		field, err := document.NewDateTimeFieldWithProperty(fieldName, parsedDateTime, f.Property())
		if err != nil {
			// todo ignore_malformed
			return err
		}
		context.doc.AddField(field)
		if !f.IncludeInAll {
			context.excludedFromAll = append(context.excludedFromAll, fieldName)
		}
	default:
		return errors.New("invalid field value")
	}
	return nil
}

type BooleanFieldMapping struct {
	Name_ string                   `json:"name,omitempty"`
	// field ID
	Id  uint64                     `json:"id,omitempty"`
	Type_ string                   `json:"type,omitempty"`
	Enabled_ bool                  `json:"enabled,omitempty"`
	Boost float64                  `json:"boost,omitempty"`
	DocValues bool                 `json:"doc_values,omitempty"`
	Index_ bool                    `json:"index,omitempty"`
	NullValue float64              `json:"null_value,omitempty"`
	Store_ bool                    `json:"store,omitempty"`
}

func NewBooleanFieldMapping(name string, id uint64) *BooleanFieldMapping {
	return &BooleanFieldMapping{
		Name_: name,
		Id: id,
		Type_: "boolean",
		Enabled_: true,
		Boost: 1.0,
		DocValues: true,
		Store_: false,
	}
}

func(f *BooleanFieldMapping) Name() string {return f.Name_}
func(f *BooleanFieldMapping) Type() string {return f.Type_}
func(f *BooleanFieldMapping) ID()   uint64 {return f.Id}
func(f *BooleanFieldMapping) Store() bool {return f.Store_}
func(f *BooleanFieldMapping) Index() bool {return f.Index_}
func(f *BooleanFieldMapping) Enabled() bool {return f.Enabled_}
func(f *BooleanFieldMapping) Property() document.Property {
	var p document.Property
	if f.Store() {
		p |= document.StoreField
	}
	if f.Index() {
		p |= document.IndexField
	}
	if f.DocValues {
		p |= document.DocValues
	}
	return p
}

func(f *BooleanFieldMapping) ParseField(data interface{}, path []string, context *parseContext) error {
	if !f.Enabled() {
		return nil
	}
	propertyValue := reflect.ValueOf(data)
	if !propertyValue.IsValid() {
		// cannot do anything with the zero value
		return errors.New("field value invalid")
	}
	propertyType := propertyValue.Type()
	switch propertyType.Kind() {
	case reflect.Bool:
		propertyValueBool := propertyValue.Bool()
		fieldName := getFieldName(path, f)
		field := document.NewBooleanFieldWithProperty(fieldName, propertyValueBool, f.Property())
		context.doc.AddField(field)
	default:
		return errors.New("invalid field type")
	}
	return nil
}

func getFieldName(path []string, fieldMapping FieldMapping) string {
	parentName := ""
	if len(path) > 1 {
		parentName = encodePath(path[:len(path)-1]) + pathSeparator
	}
	fieldName := parentName + fieldMapping.Name()
	return fieldName
}


const pathSeparator = "."

func decodePath(path string) []string {
	return strings.Split(path, pathSeparator)
}

func encodePath(pathElements []string) string {
	return strings.Join(pathElements, pathSeparator)
}
