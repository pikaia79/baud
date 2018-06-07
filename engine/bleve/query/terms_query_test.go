package query

import (
	"testing"
	"encoding/json"
	"reflect"

	"github.com/blevesearch/bleve/search/query"
)

func TestTermsQuery(t *testing.T) {
	data := `{ "user" : ["kimchy", "elasticsearch"]}`
	tq := NewTermsQuery()
	err := json.Unmarshal([]byte(data), tq)
	if err != nil {
		t.Fatal(err)
	}

	q := query.NewBooleanQuery(nil, nil, nil)
	q.SetBoost(1.0)
	terms := []string{"kimchy", "elasticsearch"}
	for _, term := range terms {
		sq := query.NewTermQuery(term)
		sq.SetBoost(1.0)
		sq.SetField("user")
		q.AddShould(sq)
	}
	q.SetMinShould(1.0)
	if !reflect.DeepEqual(q, tq.Query) {
		t.Fatal("parse terms query failed")
	}
}
