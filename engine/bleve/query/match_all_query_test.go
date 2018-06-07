package query

import (
	"testing"
	"reflect"
	"encoding/json"

	"github.com/blevesearch/bleve/search/query"
)

func TestMatchAllQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{input:`{ }`,
		output: func() query.Query {
			utq := query.NewMatchAllQuery()
			q := NewMatchAllQuery()
			q.SetQuery(utq)
			return q
		}(),},

		QueryTestGroup{input:`{ "boost" : 1.2 }`,
			output:func() query.Query {
				utq := query.NewMatchAllQuery()
				utq.SetBoost(1.2)
				q := NewMatchAllQuery()
				q.SetQuery(utq)
				return q
			}(),},
	}

	for _, group := range groups {
		tq := NewMatchAllQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
