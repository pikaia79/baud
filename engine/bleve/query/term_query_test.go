package query

import (
	"testing"
	"encoding/json"
	"reflect"

	"github.com/blevesearch/bleve/search/query"
)

type QueryTestGroup struct {
	input string
	output query.Query
	err  error
}

func TestParseTermQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{input:`{
            "status": {
              "value": "urgent",
              "boost": 2.0
            }
          }`,
		output: func() query.Query {
			utq := query.NewTermQuery("urgent")
			utq.SetField("status")
			utq.SetBoost(2.0)
			return utq
		}(),},
		QueryTestGroup{
			input:`{
            "status": "normal"
          }`,
			output: func() query.Query {
				utq := query.NewTermQuery("normal")
				utq.SetField("status")
				utq.SetBoost(1.0)
				return utq
			}(),},
	}

	for _, group := range groups {
		tq := NewTermQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		ttq, ok := tq.Query.(*query.TermQuery)
		if !ok {
			t.Fatal("parse failed")
		}
		if !reflect.DeepEqual(ttq, group.output) {
			t.Fatalf("parse failed %v %v", ttq, group.output)
		}
	}
}
