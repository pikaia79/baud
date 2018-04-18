package mapping

import (
	"testing"
	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/analysis"
	"time"
	"github.com/tiglabs/baud/util"
	"encoding/json"
)

type mockAnalyzer struct {}

func(a *mockAnalyzer)Analyze(val []byte) []*analysis.Token {
	return nil
}

func(a *mockAnalyzer)ParseDateTime(t string) (time.Time, error) {
	return time.Parse(time.RFC3339, t)
}

type mockIndexMapping struct {}

func(m *mockIndexMapping) MapDocument(doc *document.Document, data []byte) error {
	return nil
}

func(m *mockIndexMapping) AnalyzerNamed(name string) analysis.Analyzer {
	return &mockAnalyzer{}
}

func(m *mockIndexMapping) DateTimeParserNamed(name string) analysis.DateTimeParser {
	return &mockAnalyzer{}
}

func TestTextFieldMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewTextFieldMapping("text", 1)
	var path []string
	err := fieldMapping.ParseField("text string", path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	f := context.doc.Fields[0]
	if _, ok := f.(*document.TextField); !ok {
		t.Fatal("invalid failed")
	}
	if f.Name() != "text" {
		t.Fatal("invalid failed")
	}
	if string(f.Value()) != "text string" {
		t.Fatal("invalid failed")
	}
}

func TestTextFieldWithFieldsMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewTextFieldMapping("text", 1)
	fieldMapping.AddField(NewTextFieldMapping("sub", 2))
	var path []string
	err := fieldMapping.ParseField("text string", path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 2 {
		t.Fatal("ParseField failed")
	}
	f := context.doc.Fields[0]
	if _, ok := f.(*document.TextField); !ok {
		t.Fatal("invalid failed")
	}
	if f.Name() != "text" {
		t.Fatal("invalid failed")
	}
	if string(f.Value()) != "text string" {
		t.Fatal("invalid failed")
	}
	sf := context.doc.Fields[1]
	if _, ok := sf.(*document.TextField); !ok {
		t.Fatal("invalid failed")
	}
	if sf.Name() != "text.sub" {
		t.Fatal("invalid failed")
	}
	if string(sf.Value()) != "text string" {
		t.Fatal("invalid failed")
	}
}

func TestKeywordFieldMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewKeywordFieldMapping("keyword", 1)
	var path []string
	err := fieldMapping.ParseField("keyword string", path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	f := context.doc.Fields[0]
	if _, ok := f.(*document.TextField); !ok {
		t.Fatal("invalid failed")
	}
	if f.Name() != "keyword" {
		t.Fatal("invalid failed")
	}
	if string(f.Value()) != "keyword string" {
		t.Fatal("invalid failed")
	}
}

func TestDateFieldMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewDateFieldMapping("date", 1)
	var path []string
	timestamp := time.Now().UTC().Format(time.RFC3339)
	err := fieldMapping.ParseField(timestamp, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	f := context.doc.Fields[0]
	if timeField, ok := f.(*document.DateTimeField); !ok {
		t.Fatal("invalid failed")
	} else {
		tt, err := timeField.DateTime()
		if err != nil {
			t.Fatalf("invalid data time %v", err)
		}
		if tt.Format(time.RFC3339) != timestamp {
			t.Fatalf("invalid failed %s %s", tt.Format(time.RFC3339), timestamp)
		}
	}
	if f.Name() != "date" {
		t.Fatal("invalid failed")
	}
}

func TestNumericStringFieldMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewNumericFieldMapping("numeric", "integer", 1)
	var path []string
	err := fieldMapping.ParseField("123", path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	f := context.doc.Fields[0]
	if _, ok := f.(*document.NumericField); !ok {
		t.Fatal("invalid failed")
	}
	if f.Name() != "numeric" {
		t.Fatal("invalid failed")
	}
	i64, err := util.Value(f.Value()).Int64()
	if err != nil {
		t.Fatalf("invalid numeric %v", err)
	}
	f64 := util.Int64ToFloat64(i64)
	if int64(f64) != 123 {
		t.Fatalf("invalid failed %d ", i64)
	}
}

func TestNumericIntFieldMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewNumericFieldMapping("numeric", "integer", 1)
	var path []string
	err := fieldMapping.ParseField(123, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	f := context.doc.Fields[0]
	if _, ok := f.(*document.NumericField); !ok {
		t.Fatal("invalid failed")
	}
	if f.Name() != "numeric" {
		t.Fatal("invalid failed")
	}
	i64, err := util.Value(f.Value()).Int64()
	if err != nil {
		t.Fatalf("invalid numeric %v", err)
	}
	f64 := util.Int64ToFloat64(i64)
	if int64(f64) != 123 {
		t.Fatalf("invalid failed %d ", i64)
	}
}

func TestBooleanFieldMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewBooleanFieldMapping("bool", 1)
	var path []string
	err := fieldMapping.ParseField(true, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	f := context.doc.Fields[0]
	if field, ok := f.(*document.BooleanField); !ok {
		t.Fatal("invalid failed")
	} else {
		b, err := field.Boolean()
		if err != nil {
			t.Fatalf("invalid bool %v", err)
		}
		if b != true {
			t.Fatal("invalid bool")
		}
	}
	if f.Name() != "bool" {
		t.Fatal("invalid failed")
	}
}

func TestObjectFieldMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	objFieldMapping := NewObjectFieldMapping("object", 1)
	objFieldMapping.AddFileMapping(NewTextFieldMapping("text", 2))
	objFieldMapping.AddFileMapping(NewBooleanFieldMapping("bool", 3))
	objFieldMapping.AddFileMapping(NewNumericFieldMapping("number", "integer", 4))
	objFieldMapping.AddFileMapping(NewDateFieldMapping("date", 5))
	type TestObject struct {
		Text     string    `json:"text"`
		Bool     bool      `json:"bool"`
		Number   int64     `json:"number"`
		Date     string    `json:"date"`
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	obj := &TestObject{
		Text:     "text string",
		Bool:     true,
		Number:   1024,
		Date:     timestamp,
	}
	objM := make(map[string]*TestObject)
	objM["object"] = obj
	data, _ := json.Marshal(objM)
	doc := make(map[string]interface{})
	json.Unmarshal(data, &doc)
	var path []string
	err := objFieldMapping.ParseField(doc["object"], path, context)
	if err != nil {
		t.Fatalf("parse field failed, err %v", err)
	}
	if len(context.doc.Fields) != 4 {
		t.Fatal("parse field failed")
	}
	f := context.doc.FindField("object.text")
	if f == nil {
		t.Fatal("parse field failed")
	}
	f = context.doc.FindField("object.bool")
	if f == nil {
		t.Fatal("parse field failed")
	}
	f = context.doc.FindField("object.number")
	if f == nil {
		t.Fatal("parse field failed")
	}
	f = context.doc.FindField("object.date")
	if f == nil {
		t.Fatal("parse field failed")
	}
}
