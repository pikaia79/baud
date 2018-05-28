package mapping

import (
	"encoding/json"
	"reflect"
	"fmt"
	"errors"
	"strconv"
	"strings"
	"sort"
	"sync/atomic"

	"github.com/tiglabs/baudengine/kernel/document"
)

type DocumentMapping struct {
	Name string                            `json:"name,omitempty"`
	Enabled_ bool                          `json:"enabled,omitempty"`
	Dynamic bool                           `json:"dynamic,omitempty"`
	Mapping map[string]FieldMapping        `json:"mappings"`
	StructTagKey string                    `json:"-"`
}

func NewDocumentMapping(name string, mapping map[string]FieldMapping) *DocumentMapping {
	doc := &DocumentMapping{Name:name, StructTagKey: "json", Mapping: mapping, Enabled_: true, Dynamic: false}
	return doc
}

func (dm *DocumentMapping) parseDocument(doc interface{}, path []string, context *parseContext) error {
	if dm.Mapping != nil && dm.Enabled_ && !dm.Dynamic {
		val := reflect.ValueOf(doc)
		typ := val.Type()
		switch typ.Kind() {
		case reflect.Map:
			if typ.Key().Kind() == reflect.String {
				for _, key := range val.MapKeys() {
					field, ok := dm.Mapping[key.String()]
					if !ok {
						return fmt.Errorf("Fields %s that can not be identified", key.String())
					}

					fieldName := key.String()
					fieldVal := val.MapIndex(key).Interface()
					err := field.ParseField(fieldVal, append(path, fieldName), context)
					if err != nil {
						return err
					}
				}
			}
		case reflect.Slice, reflect.Array:
			for i := 0; i < val.Len(); i++ {
				if val.Index(i).CanInterface() {
					fieldVal := val.Index(i).Interface()
					err := dm.parseDocument(fieldVal, path, context)
					if err != nil {
						return err
					}
				}
			}
		default:
			return errors.New("Fields that can not be identified")
		}
		allFieldMapping := dm.Mapping["_all"]
		if allFieldMapping != nil && allFieldMapping.Enabled() {
			field := document.NewCompositeFieldWithProperty("_all", context.excludedFromAll, allFieldMapping.(*TextFieldMapping).Property())
			context.doc.AddField(field)
		}
	}
	return nil
}

var (
	mappingParameters = []string{"type", "analyzer", "normalizer", "boost", "coerce", "copy_to",
	             "doc_values", "dynamic", "enabled", "fielddata", "format", "ignore_above",
	             "ignore_malformed", "include_in_all", "index_options", "index", "fields", "norms",
	             "null_value", "position_increment_gap", "properties", "search_analyzer", "similarity", "store",
	             "term_vector", "_all", "properties", "fields"}
)

func findParameter(name string, vals []reflect.Value) (reflect.Value, bool) {
	for _, key := range vals {
		if key.String() == name {
			return key, true
		}
	}
	return reflect.Value{}, false
}

func parseBool(val interface{}) (bool, error) {
	_val := reflect.ValueOf(val)
	typ := _val.Type()
	switch typ.Kind() {
	case reflect.String:
		b := _val.String()
		if b == "true" {
			return true, nil
		}
		if b == "false" {
			return false, nil
		}
		return false, fmt.Errorf("invalid bool value %s", b)
	case reflect.Bool:
		return _val.Bool(), nil
	default:
		return false, fmt.Errorf("invalid bool value %v", val)
	}
}

func parseString(val interface{}) (string, error) {
	_val := reflect.ValueOf(val)
	typ := _val.Type()
	switch typ.Kind() {
	case reflect.String:
		return _val.String(), nil
	default:
		return "", fmt.Errorf("invalid string value %v", val)
	}
}

func parseArrayString(val interface{}) ([]string, error) {
	_val := reflect.ValueOf(val)
	typ := _val.Type()
	switch typ.Kind() {
	case reflect.Slice, reflect.Array:
		var arrayStr []string
		for i := 0; i < _val.Len(); i++ {
			if _val.Index(i).CanInterface() {
				strVal := reflect.ValueOf(_val.Index(i).Interface()).String()
				arrayStr = append(arrayStr, strVal)
			}
		}
		return arrayStr, nil
	default:
		return nil, fmt.Errorf("invalid string value %v", val)
	}
}

func parseFloat(val interface{}) (float64, error) {
	var valFloat float64
	var err error
	_val := reflect.ValueOf(val)
	typ := _val.Type()
	switch typ.Kind() {
	case reflect.String:
		valFloat, err = strconv.ParseFloat(_val.String(), 64)
		if err != nil {
			return 0.0, err
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		valFloat = float64(_val.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		valFloat = float64(_val.Uint())
	case reflect.Float64, reflect.Float32:
		valFloat = _val.Float()
	default:
		return 0.0, fmt.Errorf("invalid value type %s", typ.Kind().String())
	}
	return valFloat, nil
}

func parseInt(val interface{}) (int64, error) {
	var valInt int64
	var err error
	_val := reflect.ValueOf(val)
	typ := _val.Type()
	switch typ.Kind() {
	case reflect.String:
		valInt, err = strconv.ParseInt(_val.String(), 10, 64)
		if err != nil {
			return 0, err
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		valInt = int64(_val.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		valInt = int64(_val.Uint())
	case reflect.Float64, reflect.Float32:
		valInt = int64(_val.Float())
	default:
		return 0, fmt.Errorf("invalid value type %s", typ.Kind().String())
	}
	return valInt, nil
}

func parseFieldDataFrequencyFilter(val interface{}) (*FieldDataFrequencyFilter, error) {
	_val := reflect.ValueOf(val)
	typ := _val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			filter := &FieldDataFrequencyFilter{}
			for _, key := range _val.MapKeys() {
				if key.String() == "min" {
					f, err := parseFloat(_val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filter.Min = f
				} else if key.String() == "max" {
					f, err := parseFloat(_val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filter.Max = f
				} else if key.String() == "min_segment_size" {
					i, err := parseInt(_val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filter.MinSegmentSize = i
				} else {
					// fixme do error ??
				}
			}
			return filter, nil
		}
	}
	return nil, errors.New("invalid fielddata_frequency_filter")
}

func parseFields(val interface{}, index *uint64, enable, includeInAll bool) ([]FieldMapping, error) {
	_val := reflect.ValueOf(val)
	typ := _val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			var fields []FieldMapping
			keys := valSlice(_val.MapKeys())
			sort.Sort(keys)
			for _, key := range keys {
				// TODO keyword field
				field, err := parseStringFieldMapping(key.String(), _val.MapIndex(key).Interface(), index, enable, includeInAll)
				if err != nil {
					return nil, err
				}
				fields = append(fields, field)
			}
			return fields, nil
		}
	}
	return nil, errors.New("invalid fields")
}

func validIndexOptions(opt string) bool {
	switch opt {
	case "docs", "freqs", "positions", "offsets":
		return true
	default:
		return false
	}
	return false
}

func validFieldDataFrequencyFilter(f *FieldDataFrequencyFilter) bool {
	if f == nil {
		return false
	}
	if f.Max < f.Min {
		return false
	}
	if f.MinSegmentSize <= 0 {
		return false
	}
	return true
}

func validSimilarity(s string) bool {
	switch s {
	case "BM25", "classic", "boolean":
		return true
	default:
		return false
	}
	return false
}

func validTermVector(s string) bool {
	switch s {
	case "no", "yes", "with_positions", "with_offsets", "with_positions_offsets":
		return true
	}
	return false
}

func parseStringFieldMapping(name string, obj interface{}, index *uint64, enable, includeInAll bool) (FieldMapping, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				if key.String() == "type" {
					fTyp := reflect.ValueOf(val.MapIndex(key).Interface()).String()
					if fTyp == "text" {
						return parseTextFieldMapping(name, obj, index, enable, includeInAll)
					}else if fTyp == "keyword" {
						return parseKeyWordFieldMapping(name, obj, index, enable, includeInAll)
					}
				}
			}
		}
	}
	return nil, errors.New("invalid filed")
}

func parseTextFieldMapping(name string, obj interface{}, index *uint64, enable, includeInAll bool) (*TextFieldMapping, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			filedMapping := NewTextFieldMapping(name, atomic.AddUint64(index, 1))
			filedMapping.Enabled_ = enable
			filedMapping.IncludeInAll = includeInAll
			for _, key := range val.MapKeys() {
				switch key.String() {
				case "enabled":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Enabled_ = b
				case "analyzer":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Analyzer_ = s
				case "boost":
					f, err := parseFloat(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Boost = f
				//case "eager_global_ordinals":
				//	b, err := parseBool(val.MapIndex(key).Interface())
				//	if err != nil {
				//		return nil, err
				//	}
				case "fielddata":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.FieldData = b
				case "fielddata_frequency_filter":
					filter, err := parseFieldDataFrequencyFilter(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					if !validFieldDataFrequencyFilter(filter) {
						return nil, fmt.Errorf("invalid fielddata_frequency_filter")
					}
					filedMapping.FieldDataFrequencyFilter = filter
				case "fields":
					fields, err := parseFields(val.MapIndex(key).Interface(), index, enable, includeInAll)
					if err != nil {
						return nil, err
					}
					filedMapping.Fields = make(map[string]FieldMapping)
					for _, f := range fields {
						filedMapping.Fields[f.Name()] = f
					}
				case "index":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Index_ = b
				case "index_options":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					if !validIndexOptions(s) {
						return nil, fmt.Errorf("invalid index options %s", s)
					}
					filedMapping.IndexOptions = s
				case "norms":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Norms = b
				case "position_increment_gap":
					i, err := parseInt(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					if i <= 0 {
						return nil, errors.New("invalid position_increment_gap")
					}
					filedMapping.PositionIncrementGap = i
				case "store":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Store_ = b
				case "search_analyzer":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.SearchAnalyzer = s
				case "search_quote_analyzer":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.SearchQuoteAnalyzer = s
				case "similarity":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					if !validSimilarity(s) {
						return nil, fmt.Errorf("invalid similarity %s", s)
					}
					filedMapping.Similarity = s
				case "term_vector":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					if !validTermVector(s) {
						return nil, fmt.Errorf("invalid term_vector %s", s)
					}
                    filedMapping.TermVector = s
				case "include_in_all":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.IncludeInAll = b
				}
			}
			if len(filedMapping.SearchAnalyzer) == 0 {
				filedMapping.SearchAnalyzer = filedMapping.Analyzer_
			}
			if len(filedMapping.SearchQuoteAnalyzer) == 0 {
				filedMapping.SearchQuoteAnalyzer = filedMapping.SearchAnalyzer
			}
			return filedMapping, nil
		}
	}
	return nil, errors.New("invalid text field")
}

func parseKeyWordFieldMapping(name string, obj interface{}, index *uint64, enable, includeInAll bool) (*KeywordFieldMapping, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			filedMapping := NewKeywordFieldMapping(name, atomic.AddUint64(index, 1))
			filedMapping.Enabled_ = enable
			filedMapping.IncludeInAll = includeInAll
			for _, key := range val.MapKeys() {
				switch key.String() {
				case "enabled":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Enabled_ = b
				case "boost":
					f, err := parseFloat(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Boost = f
				//case "eager_global_ordinals":
				//	b, err := parseBool(val.MapIndex(key).Interface())
				//	if err != nil {
				//		return nil, err
				//	}
				case "doc_values":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.DocValues = b
				case "null_value":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.NullValue = s
				case "fields":
					fields, err := parseFields(val.MapIndex(key).Interface(), index, enable, includeInAll)
					if err != nil {
						return nil, err
					}
					filedMapping.Fields = make(map[string]FieldMapping)
					for _, f := range fields {
						filedMapping.Fields[f.Name()] = f
					}
				case "index":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Index_ = b
				case "index_options":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					if !validIndexOptions(s) {
						return nil, fmt.Errorf("invalid index options %s", s)
					}
					filedMapping.IndexOptions = s
				case "norms":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Norms = b
				case "store":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Store_ = b
				case "similarity":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					if !validSimilarity(s) {
						return nil, fmt.Errorf("invalid similarity %s", s)
					}
					filedMapping.Similarity = s
				case "include_in_all":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.IncludeInAll = b
				case "normalizer":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Normalizer = s
				}
			}
			return filedMapping, nil
		}
	}
	return nil, errors.New("invalid keyword field")
}

func parseNumericFieldMapping(name string, obj interface{}, index *uint64, enable, includeInAll bool) (*NumericFieldMapping, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			// get type
			var fTyp string
			for _, key := range val.MapKeys() {
				if key.String() == "type" {
					fTyp = reflect.ValueOf(val.MapIndex(key).Interface()).String()
					break
				}
			}
			if len(fTyp) == 0 {
				return nil, errors.New("miss type")
			}
			filedMapping := NewNumericFieldMapping(name, fTyp, atomic.AddUint64(index, 1))
			filedMapping.Enabled_ = enable
			filedMapping.IncludeInAll = includeInAll
			for _, key := range val.MapKeys() {
				switch key.String() {
				case "enabled":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Enabled_ = b
				case "boost":
					f, err := parseFloat(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Boost = f
				//case "eager_global_ordinals":
				//	b, err := parseBool(val.MapIndex(key).Interface())
				//	if err != nil {
				//		return nil, err
				//	}
				case "doc_values":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.DocValues = b
				case "null_value":
					s, err := parseFloat(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.NullValue = s
				case "coerce":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Coerce = b
				case "index":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Index_ = b
				case "ignore_malformed":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.IgnoreMalformed = b
				case "store":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Store_ = b
				case "include_in_all":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.IncludeInAll = b
				}
			}
			return filedMapping, nil
		}
	}
	return nil, errors.New("invalid numeric field")
}

func parseDateFieldMapping(name string, obj interface{}, index *uint64, enable, includeInAll bool) (*DateFieldMapping, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			filedMapping := NewDateFieldMapping(name, atomic.AddUint64(index, 1))
			filedMapping.Enabled_ = enable
			filedMapping.IncludeInAll = includeInAll
			for _, key := range val.MapKeys() {
				switch key.String() {
				case "enabled":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Enabled_ = b
				case "boost":
					f, err := parseFloat(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Boost = f
				//case "eager_global_ordinals":
				//	b, err := parseBool(val.MapIndex(key).Interface())
				//	if err != nil {
				//		return nil, err
				//	}
				case "doc_values":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.DocValues = b
				case "format":
					s, err := parseString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Format = s
				case "index":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Index_ = b
				case "ignore_malformed":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.IgnoreMalformed = b
				case "store":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Store_ = b
				case "include_in_all":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.IncludeInAll = b
				}
			}
			return filedMapping, nil
		}
	}
	return nil, errors.New("invalid date field")
}

func parseBooleanFieldMapping(name string, obj interface{}, index *uint64, enable, includeInAll bool) (*BooleanFieldMapping, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			filedMapping := NewBooleanFieldMapping(name, atomic.AddUint64(index, 1))
			filedMapping.Enabled_ = enable
			for _, key := range val.MapKeys() {
				switch key.String() {
				case "enabled":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Enabled_ = b
				case "boost":
					f, err := parseFloat(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Boost = f
				//case "eager_global_ordinals":
				//	b, err := parseBool(val.MapIndex(key).Interface())
				//	if err != nil {
				//		return nil, err
				//	}
				case "null_value":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.NullValue = b
				case "doc_values":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.DocValues = b
				case "index":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Index_ = b
				case "store":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Store_ = b
				}
			}
			return filedMapping, nil
		}
	}
	return nil, errors.New("invalid boolean field")
}

func parseSourceFieldMapping(obj interface{}, index *uint64) (*SourceFieldMapping, error) {
	val := reflect.ValueOf(obj)
	typ := val.Type()
	if typ.Kind() == reflect.Map {
		if typ.Key().Kind() == reflect.String {
			filedMapping := NewSourceFieldMapping(atomic.AddUint64(index, 1))
			filedMapping.Enabled_ = true
			for _, key := range val.MapKeys() {
				switch key.String() {
				case "enabled":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Enabled_ = b
				case "includes":
					ss, err := parseArrayString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Includes = ss
				case "excludes":
					ss, err := parseArrayString(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					filedMapping.Excludes = ss
				}
			}
			return filedMapping, nil
		}
	}
	return nil, errors.New("invalid boolean field")
}

func parseObjectFieldMapping(name string, schema interface{}, index *uint64, enable, includeInAll, root bool) (*ObjectFieldMapping, error) {
	val := reflect.ValueOf(schema)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		if typ.Key().Kind() == reflect.String {
			fieldMapping := NewObjectFieldMapping(name, atomic.AddUint64(index, 1))
			keys := valSlice(val.MapKeys())
			sort.Sort(keys)
			for _, key := range keys {
				switch key.String() {
				case "_all":
					if root {
						field, err := parseTextFieldMapping("_all", val.MapIndex(key).Interface(), index, enable, includeInAll)
						if err != nil {
							return nil, err
						}
						fieldMapping.AddFileMapping(field)
						includeInAll = field.Enabled()
					} else {
						// TODO error???
					}
				case "_source":
					if root {
						field, err := parseSourceFieldMapping(val.MapIndex(key).Interface(), index)
						if err != nil {
							return nil, err
						}
						fieldMapping.AddFileMapping(field)
					} else {
						// TODO error???
					}
				case "enabled":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					enable = b
				case "include_in_all":
					b, err := parseBool(val.MapIndex(key).Interface())
					if err != nil {
						return nil, err
					}
					includeInAll = b
				case "properties":
					// already sort element, so this is the last case
					fVal := val.MapIndex(key).Interface()
					_fVal := reflect.ValueOf(fVal)
					if _fVal.Type().Kind() == reflect.Map {
						_fields, err := parseFieldMapping(fVal, index, enable, includeInAll)
						if err != nil {
							return nil, err
						}
						for _, field := range _fields {
							fieldMapping.AddFileMapping(field)
						}
					}
				}
			}
			fieldMapping.Enabled_ = enable
			fieldMapping.IncludeInAll = includeInAll
			return fieldMapping, nil
		}
	}
	return nil, errors.New("invalid field")
}

func parseFieldMapping(schema interface{}, index *uint64, enable, includeInAll bool) ([]FieldMapping, error) {
	val := reflect.ValueOf(schema)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		if typ.Key().Kind() == reflect.String {
			var fields []FieldMapping
			fieldNames := valSlice(val.MapKeys())
			sort.Sort(fieldNames)
			for _, fieldName := range fieldNames {
				fieldVal := val.MapIndex(fieldName).Interface()
				fVal := reflect.ValueOf(fieldVal)
				if fVal.Type().Kind() == reflect.Map && fVal.Type().Key().Kind() == reflect.String {
					elementNames := valSlice(fVal.MapKeys())
					sort.Sort(elementNames)
					for _, elementName := range elementNames {
						elementVal := fVal.MapIndex(elementName).Interface()
						_elementVal := reflect.ValueOf(elementVal)
						if _elementVal.Type().Kind() == reflect.Map {
							if elementName.String() == "properties" {
								field, err := parseObjectFieldMapping(fieldName.String(), fieldVal, index, enable, includeInAll, false)
								if err != nil {
									return nil, err
								}
								fields = append(fields, field)
							}
						} else if _elementVal.Type().Kind() == reflect.String {
							if elementName.String() == "type" {
								switch _elementVal.String() {
								case "text":
									field, err := parseTextFieldMapping(fieldName.String(), fieldVal, index, enable, includeInAll)
									if err != nil {
										return nil, err
									}
									fields = append(fields, field)
								case "keyword":
									field, err := parseKeyWordFieldMapping(fieldName.String(), fieldVal, index, enable, includeInAll)
									if err != nil {
										return nil, err
									}
									fields = append(fields, field)
								case "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float":
									field, err := parseNumericFieldMapping(fieldName.String(), fieldVal, index, enable, includeInAll)
									if err != nil {
										return nil, err
									}
									fields = append(fields, field)
								case "date":
									field, err := parseDateFieldMapping(fieldName.String(), fieldVal, index, enable, includeInAll)
									if err != nil {
										return nil, err
									}
									fields = append(fields, field)
								case "boolean":
									field, err := parseBooleanFieldMapping(fieldName.String(), fieldVal, index, enable, includeInAll)
									if err != nil {
										return nil, err
									}
									fields = append(fields, field)
								case "object":
								case "nested":
								// TODO nested
								default:
									return nil, fmt.Errorf("invalid filed type %s", _elementVal.String())
								}
							}
						}
					}
				}
			}
			return fields, nil
		}
	}
	return nil, errors.New("invalid field")
}

func parseSchema(data []byte) ([]*DocumentMapping, error) {
	schema := make(map[string]interface{})
	err := json.Unmarshal(data, &schema)
	if err != nil {
		return nil, err
	}
	if val, ok := schema["mappings"]; ok {
		var index uint64
		docVal := reflect.ValueOf(val)
		typ := docVal.Type()
		switch typ.Kind() {
		case reflect.Map:
			if typ.Key().Kind() == reflect.String {
				var docMappings []*DocumentMapping
				for _, docName := range docVal.MapKeys() {
					docMapping := docVal.MapIndex(docName).Interface()
					dMapping := reflect.ValueOf(docMapping)
					if dMapping.Type().Kind() == reflect.Map && dMapping.Type().Key().Kind() == reflect.String {
						objectFieldMapping, err := parseObjectFieldMapping(docName.String(), docMapping, &index, true, true, true)
						if err != nil {
							return nil, err
						}
						// _all check
						if _, ok := objectFieldMapping.Properties["_all"]; !ok {
							NewTextFieldMapping("_all", atomic.AddUint64(&index, 1))
						}
						// _source check
						docMappings = append(docMappings, NewDocumentMapping(docName.String(), objectFieldMapping.Properties))
					}
				}
				return docMappings, nil
			}
		}
	}
	return nil, errors.New("invalid schema")
}

type valSlice []reflect.Value

func (p valSlice) Len() int {
	return len(p)
}

func (p valSlice) Swap(i int, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p valSlice) Less(i int, j int) bool {
	return strings.Compare(p[i].String(), p[j].String()) < 0
}

type parseContext struct {
	doc             *document.Document
	im              IndexMapping
	dm              *DocumentMapping
	excludedFromAll []string
}