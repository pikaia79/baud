package mapping

import (
	"testing"
	"github.com/tiglabs/baud/kernel/document"
	"github.com/tiglabs/baud/kernel/analysis"
	"time"
	"github.com/tiglabs/baud/util"
	"encoding/json"
	"fmt"
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

func(m *mockIndexMapping) RebuildAllField(doc *document.Document) error {return nil}
func(m *mockIndexMapping) MergeDocument(doc *document.Document, source []byte) error {return nil}

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
	fs := context.doc.Fields["text"]
	f := fs[0]
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
	fs := context.doc.Fields["text"]
	f := fs[0]
	if _, ok := f.(*document.TextField); !ok {
		t.Fatal("invalid failed")
	}
	if f.Name() != "text" {
		t.Fatal("invalid failed")
	}
	if string(f.Value()) != "text string" {
		t.Fatal("invalid failed")
	}
	sfs := context.doc.Fields["text.sub"]
	sf := sfs[0]
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

func TestTextFieldsMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewTextFieldMapping("text", 1)
	var path []string
	checkStrings := []string{"str1", "str2"}
	err := fieldMapping.ParseField(checkStrings, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	fs := context.doc.Fields["text"]
	if len(fs) != 2 {
		t.Fatalf("parse filed failed %v", fs)
	}

	for i, str := range checkStrings {
		f := fs[i]
		if _, ok := f.(*document.TextField); !ok {
			t.Fatal("invalid failed")
		}
		if f.Name() != "text" {
			t.Fatal("invalid failed")
		}
		if string(f.Value()) != str {
			t.Fatal("invalid failed")
		}
	}

}

func TestTextFieldsWithFieldsMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewTextFieldMapping("text", 1)
	fieldMapping.AddField(NewTextFieldMapping("sub", 2))
	var path []string
	checkStrings := []string{"str1", "str2"}
	err := fieldMapping.ParseField(checkStrings, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 2 {
		t.Fatal("ParseField failed")
	}
	fieldNames := []string{"text", "text.sub"}
	for _, fn := range fieldNames {
		fs := context.doc.Fields[fn]
		if len(fs) != 2 {
			t.Fatalf("parse filed failed %v", fs)
		}

		for i, str := range checkStrings {
			f := fs[i]
			if _, ok := f.(*document.TextField); !ok {
				t.Fatal("invalid failed")
			}
			if f.Name() != fn {
				t.Fatal("invalid failed")
			}
			if string(f.Value()) != str {
				t.Fatal("invalid failed")
			}
		}
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
	fs := context.doc.Fields["keyword"]
	f := fs[0]
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

func TestKeywordFieldsMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewKeywordFieldMapping("keyword", 1)
	var path []string
	checkStrings := []string{"str1", "str2"}
	err := fieldMapping.ParseField(checkStrings, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	fs := context.doc.Fields["keyword"]
	if len(fs) != 2 {
		t.Fatalf("parse filed failed %v", fs)
	}

	for i, str := range checkStrings {
		f := fs[i]
		if _, ok := f.(*document.TextField); !ok {
			t.Fatal("invalid failed")
		}
		if f.Name() != "keyword" {
			t.Fatal("invalid failed")
		}
		if string(f.Value()) != str {
			t.Fatal("invalid failed")
		}
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
	fs := context.doc.Fields["date"]
	f := fs[0]
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

func TestDateFieldsMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewDateFieldMapping("date", 1)
	var path []string
	var timestamps []string
	now := time.Now().UTC()
	timestamps = append(timestamps, now.Format(time.RFC3339))
	timestamps = append(timestamps, now.Add(time.Second).Format(time.RFC3339))
	err := fieldMapping.ParseField(timestamps, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	fs := context.doc.Fields["date"]
	for i, ts := range timestamps {
		f := fs[i]
		if timeField, ok := f.(*document.DateTimeField); !ok {
			t.Fatal("invalid failed")
		} else {
			tt, err := timeField.DateTime()
			if err != nil {
				t.Fatalf("invalid data time %v", err)
			}
			if tt.Format(time.RFC3339) != ts {
				t.Fatalf("invalid failed %s %s", tt.Format(time.RFC3339), ts)
			}
		}
		if f.Name() != "date" {
			t.Fatal("invalid failed")
		}
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
	fs := context.doc.Fields["numeric"]
	f := fs[0]
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

func TestNumericStringFieldsMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewNumericFieldMapping("numeric", "integer", 1)
	var path []string
	nums := []string{"123", "456"}
	err := fieldMapping.ParseField(nums, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	fs := context.doc.Fields["numeric"]
	for i, n := range nums {
		f := fs[i]
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
		if fmt.Sprintf("%d", int64(f64)) != n {
			t.Fatalf("invalid failed %d ", i64)
		}
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
	fs := context.doc.Fields["numeric"]
	f := fs[0]
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

func TestBooleanFieldsMapping(t *testing.T) {
	context := &parseContext{
		doc: document.NewDocument("1"),
		im: &mockIndexMapping{},
		dm: nil,
	}
	fieldMapping := NewBooleanFieldMapping("bool", 1)
	var path []string
	bools := []bool{true, false}
	err := fieldMapping.ParseField(bools, path, context)
	if err != nil {
		t.Fatalf("ParseField failed, %v", err)
	}
	if len(context.doc.Fields) != 1 {
		t.Fatal("ParseField failed")
	}
	fs := context.doc.Fields["bool"]
	for i, bool := range bools {
		f := fs[i]
		if field, ok := f.(*document.BooleanField); !ok {
			t.Fatal("invalid failed")
		} else {
			b, err := field.Boolean()
			if err != nil {
				t.Fatalf("invalid bool %v", err)
			}
			if b != bool {
				t.Fatal("invalid bool")
			}
		}
		if f.Name() != "bool" {
			t.Fatal("invalid failed")
		}
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
	fs := context.doc.Fields["bool"]
	f := fs[0]
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
	f := context.doc.FindFields("object.text")
	if f == nil {
		t.Fatal("parse field failed")
	}
	f = context.doc.FindFields("object.bool")
	if f == nil {
		t.Fatal("parse field failed")
	}
	f = context.doc.FindFields("object.number")
	if f == nil {
		t.Fatal("parse field failed")
	}
	f = context.doc.FindFields("object.date")
	if f == nil {
		t.Fatal("parse field failed")
	}
}

func TestObjectFieldsMapping(t *testing.T) {
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

	var objs []*TestObject
	timestamp1 := time.Now().UTC().Format(time.RFC3339)
	obj1 := &TestObject{
		Text:     "text string",
		Bool:     true,
		Number:   1024,
		Date:     timestamp1,
	}
	objs = append(objs, obj1)
	timestamp2 := time.Now().UTC().Add(time.Second).Format(time.RFC3339)
	obj2 := &TestObject{
		Text:     "mutli text string",
		Bool:     false,
		Number:   2048,
		Date:     timestamp2,
	}
	objs = append(objs, obj2)
	objM := make(map[string]interface{})
	objM["object"] = objs
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
	fts := context.doc.FindFields("object.text")
	if fts == nil || len(fts) != 2{
		t.Fatal("parse field failed")
	}
	fbs := context.doc.FindFields("object.bool")
	if fbs == nil || len(fbs) != 2{
		t.Fatal("parse field failed")
	}
	fns := context.doc.FindFields("object.number")
	if fns == nil || len(fns) != 2{
		t.Fatal("parse field failed")
	}
	fds := context.doc.FindFields("object.date")
	if fds == nil || len(fds) != 2 {
		t.Fatal("parse field failed")
	}
	for i, obj := range objs {
		ft := fts[i]
		if string(ft.Value()) != obj.Text {
			t.Fatalf("invalid field %v", ft)
		}
		fb := fbs[i]
		bool, err := fb.(*document.BooleanField).Boolean()
		if err != nil {
			t.Fatalf("invalid field %v %v", fb, err)
		}
		if bool != obj.Bool {
			t.Fatalf("invalid field %v", fb)
		}
		fn := fns[i]
		if _, ok := fn.(*document.NumericField); !ok {
			t.Fatal("invalid failed")
		}
		if fn.Name() != "object.number" {
			t.Fatalf("invalid failed ")
		}
		i64, err := util.Value(fn.Value()).Int64()
		if err != nil {
			t.Fatalf("invalid numeric %v", err)
		}
		f64 := util.Int64ToFloat64(i64)
		if int64(f64) != obj.Number {
			t.Fatalf("invalid failed %d ", i64)
		}

		fd := fds[i]
		if timeField, ok := fd.(*document.DateTimeField); !ok {
			t.Fatal("invalid failed")
		} else {
			tt, err := timeField.DateTime()
			if err != nil {
				t.Fatalf("invalid data time %v", err)
			}
			if tt.Format(time.RFC3339) != obj.Date {
				t.Fatalf("invalid failed %s %s", tt.Format(time.RFC3339), obj.Date)
			}
		}
		if fd.Name() != "object.date" {
			t.Fatal("invalid failed")
		}
	}
}
