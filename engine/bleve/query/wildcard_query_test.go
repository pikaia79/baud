package query

import (
	"testing"
	"reflect"
	"encoding/json"

	"github.com/blevesearch/bleve/search/query"
)

func TestWildcardQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{input:`{ "user" : "ki*y" }`,
		output:func() query.Query {
			utq := query.NewWildcardQuery("ki*y")
			utq.SetField("user")
			utq.SetBoost(1.0)
			return utq
		}(),},
		QueryTestGroup{
			input:`{ "user" : { "value" : "ki*y", "boost" : 2.0 } }`,
			output:func() query.Query {
				utq := query.NewWildcardQuery("ki*y")
				utq.SetField("user")
				utq.SetBoost(2.0)
				return utq
			}(),},

		QueryTestGroup{
			input:`{ "user" : { "wildcard" : "ki*y", "boost" : 2.0 } }`,
			output:func() query.Query {
				utq := query.NewWildcardQuery("ki*y")
				utq.SetField("user")
				utq.SetBoost(2.0)
				return utq
			}(),},
	}

	for _, group := range groups {
		tq := NewWildcardQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq.Query, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}