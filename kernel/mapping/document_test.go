package mapping

import (
	"testing"
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
	t.Log(field)
}