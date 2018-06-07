package query

import (
	"testing"
	"reflect"
	"encoding/json"

	"github.com/blevesearch/bleve/search/query"
)

func TestRegexpQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{input:`{
        "name.first": "s.*y"
    }`,
		output:func() query.Query {
			utq := query.NewRegexpQuery("s.*y")
			utq.SetField("name.first")
			utq.SetBoost(1.0)
			q := NewRegexpQuery()
			q.SetQuery(utq)
			return q
		}(),},
		QueryTestGroup{
			input:`{
        "name.first":{
            "value":"s.*y",
            "boost":1.2
        }
    }`,
			output:func() query.Query {
				utq := query.NewRegexpQuery("s.*y")
				utq.SetField("name.first")
				utq.SetBoost(1.2)
				q := NewRegexpQuery()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{
			input:`{
        "name.first": {
            "value": "s.*y",
            "flags" : "INTERSECTION|COMPLEMENT|EMPTY"
        }
    }`,
			output:func() query.Query {
				utq := query.NewRegexpQuery("s.*y")
				utq.SetField("name.first")
				utq.SetBoost(1.0)
				q := NewRegexpQuery()
				q.SetQuery(utq)
				q.SetFlags("INTERSECTION|COMPLEMENT|EMPTY")
				return q
			}(),},

		QueryTestGroup{
			input:`{
        "name.first": {
            "value": "s.*y",
            "flags" : "INTERSECTION|COMPLEMENT|EMPTY",
            "max_determinized_states": 20000
        }
    }`,
			output:func() query.Query {
				utq := query.NewRegexpQuery("s.*y")
				utq.SetField("name.first")
				utq.SetBoost(1.0)
				q := NewRegexpQuery()
				q.SetQuery(utq)
				q.SetFlags("INTERSECTION|COMPLEMENT|EMPTY")
				q.SetMaxDeterminizedStates(20000)
				return q
			}(),},
	}

	for _, group := range groups {
		tq := NewRegexpQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
