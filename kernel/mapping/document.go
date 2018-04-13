package mapping

import (
	"time"
	"reflect"
	"encoding"
)

type DocumentMapping struct {
	Enabled bool                           `json:enabled`
	Dynamic bool                           `json:"dynamic"`
	Properties map[string]*DocumentMapping `json:"properties,omitempty"`
	Fields []*FieldMapping                 `json:"fields,omitempty"`
	DefaultAnalyzer string                 `json:"default_analyzer"`

	// Default tag key is `json`
	StructTagKey string                    `json:"struct_tag_key,omitempty"`
}

func NewDocumentMapping() *DocumentMapping {
	return *DocumentMapping{
		Enabled: true,
		Dynamic: false,
		DefaultAnalyzer: "standard",
		StructTagKey: "json",
	}
}

func (d *DocumentMapping) AddFieldMapping(field *FieldMapping) {
	d.Fields = append(d.Fields, field)
}

func (d *DocumentMapping) AddProperty(property string, dm *DocumentMapping) {
	if d.Properties == nil {
		d.Properties = make(map[string]*DocumentMapping)
	}
	// if the property already exist, overwrite
	d.Properties[property] = dm
}


func (dm *DocumentMapping) parseDocument(doc interface{}, path []string, indexes []uint64, context *walkContext) {
	structTagKey := dm.StructTagKey
	if structTagKey == "" {
		structTagKey = "json"
	}

	val := reflect.ValueOf(doc)
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Map:
		if typ.Key().Kind() == reflect.String {
			for _, key := range val.MapKeys() {
				fieldName := key.String()
				fieldVal := val.MapIndex(key).Interface()
				dm.processProperty(fieldVal, append(path, fieldName), indexes, context)
			}
		}
	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			field := typ.Field(i)
			fieldName := field.Name
			// anonymous fields of type struct can elide the type name
			if field.Anonymous && field.Type.Kind() == reflect.Struct {
				fieldName = ""
			}

			// if the field has a name under the specified tag, prefer that
			tag := field.Tag.Get(structTagKey)
			tagFieldName := parseTagName(tag)
			if tagFieldName == "-" {
				continue
			}
			// allow tag to set field name to empty, only if anonymous
			if field.Tag != "" && (tagFieldName != "" || field.Anonymous) {
				fieldName = tagFieldName
			}

			if val.Field(i).CanInterface() {
				fieldVal := val.Field(i).Interface()
				newpath := path
				if fieldName != "" {
					newpath = append(path, fieldName)
				}
				dm.processProperty(fieldVal, newpath, indexes, context)
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			if val.Index(i).CanInterface() {
				fieldVal := val.Index(i).Interface()
				dm.processProperty(fieldVal, path, append(indexes, uint64(i)), context)
			}
		}
	case reflect.Ptr:
		ptrElem := val.Elem()
		if ptrElem.IsValid() && ptrElem.CanInterface() {
			dm.processProperty(ptrElem.Interface(), path, indexes, context)
		}
	case reflect.String:
		dm.processProperty(val.String(), path, indexes, context)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dm.processProperty(float64(val.Int()), path, indexes, context)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dm.processProperty(float64(val.Uint()), path, indexes, context)
	case reflect.Float32, reflect.Float64:
		dm.processProperty(float64(val.Float()), path, indexes, context)
	case reflect.Bool:
		dm.processProperty(val.Bool(), path, indexes, context)
	}

}

func (dm *DocumentMapping) processProperty(property interface{}, path []string, indexes []uint64, context *walkContext) {
	pathString := encodePath(path)
	// look to see if there is a mapping for this field
	subDocMapping := dm.documentMappingForPath(pathString)
	closestDocMapping := dm.closestDocMapping(pathString)

	// check to see if we even need to do further processing
	if subDocMapping != nil && !subDocMapping.Enabled {
		return
	}

	propertyValue := reflect.ValueOf(property)
	if !propertyValue.IsValid() {
		// cannot do anything with the zero value
		return
	}
	propertyType := propertyValue.Type()
	switch propertyType.Kind() {
	case reflect.String:
		propertyValueString := propertyValue.String()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				fieldMapping.processString(propertyValueString, pathString, path, indexes, context)
			}
		} else if closestDocMapping.Dynamic {
			// automatic indexing behavior

			// first see if it can be parsed by the default date parser
			dateTimeParser := context.im.DateTimeParserNamed(context.im.DefaultDateTimeParser)
			if dateTimeParser != nil {
				parsedDateTime, err := dateTimeParser.ParseDateTime(propertyValueString)
				if err != nil {
					// index as text
					fieldMapping := newTextFieldMappingDynamic(context.im)
					fieldMapping.processString(propertyValueString, pathString, path, indexes, context)
				} else {
					// index as datetime
					fieldMapping := newDateTimeFieldMappingDynamic(context.im)
					fieldMapping.processTime(parsedDateTime, pathString, path, indexes, context)
				}
			}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		dm.processProperty(float64(propertyValue.Int()), path, indexes, context)
		return
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		dm.processProperty(float64(propertyValue.Uint()), path, indexes, context)
		return
	case reflect.Float64, reflect.Float32:
		propertyValFloat := propertyValue.Float()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				fieldMapping.processFloat64(propertyValFloat, pathString, path, indexes, context)
			}
		} else if closestDocMapping.Dynamic {
			// automatic indexing behavior
			fieldMapping := newNumericFieldMappingDynamic(context.im)
			fieldMapping.processFloat64(propertyValFloat, pathString, path, indexes, context)
		}
	case reflect.Bool:
		propertyValBool := propertyValue.Bool()
		if subDocMapping != nil {
			// index by explicit mapping
			for _, fieldMapping := range subDocMapping.Fields {
				fieldMapping.processBoolean(propertyValBool, pathString, path, indexes, context)
			}
		} else if closestDocMapping.Dynamic {
			// automatic indexing behavior
			fieldMapping := newBooleanFieldMappingDynamic(context.im)
			fieldMapping.processBoolean(propertyValBool, pathString, path, indexes, context)
		}
	case reflect.Struct:
		switch property := property.(type) {
		case time.Time:
			// don't descend into the time struct
			if subDocMapping != nil {
				// index by explicit mapping
				for _, fieldMapping := range subDocMapping.Fields {
					fieldMapping.processTime(property, pathString, path, indexes, context)
				}
			} else if closestDocMapping.Dynamic {
				fieldMapping := newDateTimeFieldMappingDynamic(context.im)
				fieldMapping.processTime(property, pathString, path, indexes, context)
			}
		case encoding.TextMarshaler:
			txt, err := property.MarshalText()
			if err == nil && subDocMapping != nil {
				// index by explicit mapping
				for _, fieldMapping := range subDocMapping.Fields {
					if fieldMapping.Type == "text" {
						fieldMapping.processString(string(txt), pathString, path, indexes, context)
					}
				}
			}
			dm.parseDocument(property, path, indexes, context)
		default:
			if subDocMapping != nil {
				for _, fieldMapping := range subDocMapping.Fields {
					if fieldMapping.Type == "geopoint" {
						fieldMapping.processGeoPoint(property, pathString, path, indexes, context)
					}
				}
			}
			dm.parseDocument(property, path, indexes, context)
		}
	case reflect.Map, reflect.Slice:
		if subDocMapping != nil {
			for _, fieldMapping := range subDocMapping.Fields {
				if fieldMapping.Type == "geopoint" {
					fieldMapping.processGeoPoint(property, pathString, path, indexes, context)
				}
			}
		}
		dm.parseDocument(property, path, indexes, context)
	case reflect.Ptr:
		if !propertyValue.IsNil() {
			switch property := property.(type) {
			case encoding.TextMarshaler:
				txt, err := property.MarshalText()
				if err == nil && subDocMapping != nil {
					// index by explicit mapping
					for _, fieldMapping := range subDocMapping.Fields {
						if fieldMapping.Type == "text" {
							fieldMapping.processString(string(txt), pathString, path, indexes, context)
						}
					}
				} else {
					dm.parseDocument(property, path, indexes, context)
				}

			default:
				dm.parseDocument(property, path, indexes, context)
			}
		}
	default:
		dm.parseDocument(property, path, indexes, context)
	}
}
