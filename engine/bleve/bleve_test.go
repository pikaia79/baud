package bleve

import (
	"testing"
	"github.com/tiglabs/baudengine/engine"
	"os"
	"golang.org/x/net/context"
)

var path = "/tmp/baud"


func clear() {
	os.Remove(path)
}

func blever(t *testing.T, schema string) engine.Engine {
	cfg := engine.EngineConfig{
		Path: path,
		ReadOnly: false,
		Schema: schema,
	}
	index, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func TestWriteDoc(t *testing.T) {
	clear()
	schema := `{
  "mappings": {
    "baud": {
      "properties": {
        "name": {
          "type": "string"
        },
        "age": {
          "type": "integer"
        }
      }
    }
  }
}`
	index := blever(t, schema)
	defer func() {
		index.Close()
		clear()
	}()
	index.AddDocument(context.Background(), engine.DOC_ID("doc1"), )
}
