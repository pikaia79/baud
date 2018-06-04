package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
)

func TestPrefixQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{`{ "user" : "ki" }`,
		func() query.Query {
			utq := query.NewPrefixQuery("ki")
			utq.SetField("user")
			utq.SetBoost(1.0)
			return utq
		}(),},
		QueryTestGroup{
			`{ "user" :  { "value" : "ki", "boost" : 2.0 } }`,
			func() query.Query {
				utq := query.NewPrefixQuery("ki")
				utq.SetField("user")
				utq.SetBoost(2.0)
				return utq
			}(),},

		QueryTestGroup{
			`{ "user" :  { "prefix" : "ki", "boost" : 2.0 } }`,
			func() query.Query {
				utq := query.NewPrefixQuery("ki")
				utq.SetField("user")
				utq.SetBoost(2.0)
				return utq
			}(),},
	}

	for _, group := range groups {
		tq := NewPrefixQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq.Query, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
