package mapping

import (
	"testing"
	"strings"
	"fmt"
)

func TestParseTextFieldMapping(t *testing.T) {
	textField := make(map[string]interface{})
	textField["type"] = "text"
	textField["include_in_all"] = false
	var index uint64
	field, err := parseTextFieldMapping("tField", textField, &index, true, true)
	if err != nil {
		t.Fatalf("parse text fielld failed %v", err)
		return
	}
	t.Log(field)
	if field.Name() != "tField" {
		t.Fatal("invalid field")
	}
	if field.Type() != "text" {
		t.Fatal("invalid field")
	}
	if field.ID() != 1 {
		t.Fatal("invalid field")
	}
	if field.IncludeInAll != false {
		t.Fatal("invalid field")
	}
}

func TestParseKeyWordFieldMapping(t *testing.T) {
	kwField := make(map[string]interface{})
	kwField["type"] = "keyword"
	kwField["include_in_all"] = false
	kwField["null_value"] = "kk"
	kwField["doc_values"] = false
	var index uint64
	field, err := parseKeyWordFieldMapping("kField", kwField, &index, true, true)
	if err != nil {
		t.Fatalf("parse text fielld failed %v", err)
		return
	}
	t.Log(field)
	if field.Name() != "kField" {
		t.Fatal("invalid field")
	}
	if field.Type() != "keyword" {
		t.Fatal("invalid field")
	}
	if field.ID() != 1 {
		t.Fatal("invalid field")
	}
	if field.IncludeInAll != false {
		t.Fatal("invalid field")
	}
	if field.NullValue != "kk" {
		t.Fatal("invalid field")
	}
	if field.DocValues != false {
		t.Fatal("invalid field")
	}
}

func TestParseNumericFieldMapping(t *testing.T) {
	numField := make(map[string]interface{})
	numField["type"] = "integer"
	numField["include_in_all"] = false
	numField["null_value"] = "123"
	numField["doc_values"] = false
	var index uint64
	field, err := parseNumericFieldMapping("nField", numField, &index, true, true)
	if err != nil {
		t.Fatalf("parse text fielld failed %v", err)
		return
	}
	t.Log(field)
	if field.Name() != "nField" {
		t.Fatal("invalid field")
	}
	if field.Type() != "integer" {
		t.Fatal("invalid field")
	}
	if field.ID() != 1 {
		t.Fatal("invalid field")
	}
	if field.IncludeInAll != false {
		t.Fatal("invalid field")
	}
	if field.NullValue != 123 {
		t.Fatal("invalid field")
	}
	if field.DocValues != false {
		t.Fatal("invalid field")
	}
}

func TestParseDateFieldMapping(t *testing.T) {
	dateField := make(map[string]interface{})
	dateField["type"] = "date"
	dateField["include_in_all"] = false
	dateField["doc_values"] = false
	dateField["format"] = "YY:MM:DD HH:MM:SS"
	var index uint64
	field, err := parseDateFieldMapping("dField", dateField, &index, true, true)
	if err != nil {
		t.Fatalf("parse text fielld failed %v", err)
		return
	}
	t.Log(field)
	if field.Name() != "dField" {
		t.Fatal("invalid field")
	}
	if field.Type() != "date" {
		t.Fatal("invalid field")
	}
	if field.ID() != 1 {
		t.Fatal("invalid field")
	}
	if field.IncludeInAll != false {
		t.Fatal("invalid field")
	}
	if field.Format != "YY:MM:DD HH:MM:SS" {
		t.Fatal("invalid field")
	}
	if field.DocValues != false {
		t.Fatal("invalid field")
	}
}

func TestParseBoolFieldMapping(t *testing.T) {
	boolField := make(map[string]interface{})
	boolField["type"] = "boolean"
	boolField["doc_values"] = false
	boolField["null_value"] = true
	var index uint64
	field, err := parseBooleanFieldMapping("bField", boolField, &index, true, true)
	if err != nil {
		t.Fatalf("parse text fielld failed %v", err)
		return
	}
	t.Log(field)
	if field.Name() != "bField" {
		t.Fatal("invalid field")
	}
	if field.Type() != "boolean" {
		t.Fatal("invalid field")
	}
	if field.ID() != 1 {
		t.Fatal("invalid field")
	}
	if field.NullValue != true {
		t.Fatal("invalid field")
	}
	if field.DocValues != false {
		t.Fatal("invalid field")
	}
}

func TestParseObjectFieldMapping(t *testing.T) {
	objField := make(map[string]interface{})
	objField["include_in_all"] = false
	textField := make(map[string]interface{})
	textField["type"] = "text"
	textField["include_in_all"] = false
	kwField := make(map[string]interface{})
	kwField["type"] = "keyword"
	kwField["include_in_all"] = false
	kwField["null_value"] = "kk"
	kwField["doc_values"] = false
	numField := make(map[string]interface{})
	numField["type"] = "integer"
	numField["include_in_all"] = false
	numField["null_value"] = "123"
	numField["doc_values"] = false
	dateField := make(map[string]interface{})
	dateField["type"] = "date"
	dateField["include_in_all"] = false
	dateField["doc_values"] = false
	dateField["format"] = "YY:MM:DD HH:MM:SS"
	boolField := make(map[string]interface{})
	boolField["type"] = "boolean"
	boolField["doc_values"] = false
	boolField["null_value"] = true
	properties := make(map[string]interface{})
	properties["textField"] = textField
	properties["kwField"] = kwField
	properties["numField"] = numField
	properties["dateField"] = dateField
	properties["boolField"] = boolField
	objField["properties"] = properties
	var index uint64
	field, err := parseObjectFieldMapping("objField", objField, &index, true, true)
	if err != nil {
		t.Fatalf("parse object fielld failed %v", err)
		return
	}
	if f, ok := field.Properties["boolField"]; ok {
		if f.Type() != "boolean" {
			t.Fatalf("parse object field failed %s", f.Type())
		}
		if f.ID() != 2 {
			t.Fatalf("parse object field failed %d", f.ID())
		}
	} else {
		t.Fatal("parse object field failed")
	}

	if f, ok := field.Properties["dateField"]; ok {
		if f.ID() != 3 {
			t.Fatalf("parse object field failed %d", f.ID())
		}
	} else {
		t.Fatal("parse object field failed")
	}

	if f, ok := field.Properties["kwField"]; ok {
		if f.ID() != 4 {
			t.Fatalf("parse object field failed %d", f.ID())
		}
	} else {
		t.Fatal("parse object field failed")
	}

	if f, ok := field.Properties["numField"]; ok {
		if f.ID() != 5 {
			t.Fatalf("parse object field failed %d", f.ID())
		}
	} else {
		t.Fatal("parse object field failed")
	}

	if f, ok := field.Properties["textField"]; ok {
		if f.ID() != 6 {
			t.Fatalf("parse object field failed %d", f.ID())
		}
	} else {
		t.Fatal("parse object field failed")
	}
}

func TestParseSchema(t *testing.T) {
	schema := `{
  "mappings": {
    "_doc": {
      "properties": {
        "region": {
          "type": "keyword"
        },
        "is_published": {
          "type": "boolean"
        },
        "title": {
          "type": "text",
          "store": true
        },
        "date": {
          "type": "date",
          "store": true
        },
        "content": {
          "type": "text"
        },
        "city": {
          "type": "text",
          "fields": {
            "raw": {
              "type":  "keyword"
            }
          }
        },
        "manager": {
          "properties": {
            "age":  { "type": "integer" },
            "name": {
              "properties": {
                "first": { "type": "text" },
                "last":  { "type": "text" }
              }
            }
          }
        }
      }
    }
  }
}
`
	//level0FieldNames := []string{"region", "is_published", "title", "date", "content", "city", "manager"}
	dm, err := parseSchema([]byte(schema))
	if err != nil {
		t.Fatal("parseSchema failed ", err)
		return
	}
	if len(dm.Mapping) != 7 {
		t.Fatalf("parse failed %v", dm.Mapping)
	}
	for fn, field := range dm.Mapping {
		switch fn {
		case "region":
			if f, ok := field.(*KeywordFieldMapping); !ok {
				t.Fatal("invalid field")
				return
			} else {
				if f.Type() != "keyword" {
					t.Fatal("invalid field")
					return
				}
				t.Log(f)
			}
		case "is_published":
			if f, ok := field.(*BooleanFieldMapping); !ok {
				t.Fatal("invalid field")
				return
			} else {
				if f.Type() != "boolean" {
					t.Fatal("invalid field")
					return
				}
				t.Log(f)
			}
		case "title":
			if f, ok := field.(*TextFieldMapping); !ok {
				t.Fatal("invalid field")
				return
			} else {
				if f.Type() != "text" {
					t.Fatal("invalid field")
					return
				}
				if f.Store() != true {
					t.Fatal("invalid field")
					return
				}
				t.Log(f)
			}
		case "date":
			if f, ok := field.(*DateFieldMapping); !ok {
				t.Fatal("invalid field")
				return
			} else {
				if f.Type() != "date" {
					t.Fatal("invalid field")
					return
				}
				if f.Store() != true {
					t.Fatal("invalid field")
					return
				}
				t.Log(f)
			}
		case "content":
			if f, ok := field.(*TextFieldMapping); !ok {
				t.Fatal("invalid field")
				return
			} else {
				if f.Type() != "text" {
					t.Fatal("invalid field")
					return
				}
				t.Log(f)
			}
		case "city":
			if f, ok := field.(*TextFieldMapping); !ok {
				t.Fatal("invalid field")
				return
			} else {
				if f.Type() != "text" {
					t.Fatal("invalid field")
					return
				}
				if f.ID() != 2 {
					t.Fatal("invalid field")
					return
				}
				if subf, ok := f.Fields["raw"]; !ok {
					t.Fatal("invalid field")
					return
				} else {
					if sf, ok := subf.(*KeywordFieldMapping); !ok {
						t.Fatal("invalid field")
						return
					} else {
						if sf.ID() != 3 {
							t.Fatal("invalid field")
							return
						}
						t.Log(sf)
					}
				}
				t.Log(f)
			}
		case "manager":
			if f, ok := field.(*ObjectFieldMapping); !ok {
				t.Fatal("invalid field")
				return
			} else {
				if f.Type() != "object" {
					t.Fatal("invalid field")
					return
				}
				t.Log(f)
				for subf, f := range f.Properties {
					switch subf {
					case "age":
						if field, ok := f.(*NumericFieldMapping); !ok {
							t.Fatal("invalid field")
							return
						} else {
							if field.ID() != 8 {
								t.Fatal("invalid field")
								return
							}
						}
					case "name":
						if field, ok := f.(*ObjectFieldMapping); !ok {
							t.Fatal("invalid field")
							return
						} else {
							if field.ID() != 9 {
								t.Fatal("invalid field")
								return
							}
						}
					}
				}
			}

		default:
			t.Fatalf("invalid field name %s", fn)
			return
		}
	}
	fmt.Println(strings.Compare("_all", "a"))
}
