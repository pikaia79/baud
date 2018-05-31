package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
)

func TestRegexpQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{`{
        "name.first": "s.*y"
    }`,
		func() query.Query {
			utq := query.NewRegexpQuery("s.*y")
			utq.SetField("name.first")
			utq.SetBoost(1.0)
			q := NewRegexpQuery()
			q.SetQuery(utq)
			return q
		}(),},
		QueryTestGroup{
			`{
        "name.first":{
            "value":"s.*y",
            "boost":1.2
        }
    }`,
			func() query.Query {
				utq := query.NewRegexpQuery("s.*y")
				utq.SetField("name.first")
				utq.SetBoost(1.2)
				q := NewRegexpQuery()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{
			`{
        "name.first": {
            "value": "s.*y",
            "flags" : "INTERSECTION|COMPLEMENT|EMPTY"
        }
    }`,
			func() query.Query {
				utq := query.NewRegexpQuery("s.*y")
				utq.SetField("name.first")
				utq.SetBoost(1.0)
				q := NewRegexpQuery()
				q.SetQuery(utq)
				q.SetFlags("INTERSECTION|COMPLEMENT|EMPTY")
				return q
			}(),},

		QueryTestGroup{
			`{
        "name.first": {
            "value": "s.*y",
            "flags" : "INTERSECTION|COMPLEMENT|EMPTY",
            "max_determinized_states": 20000
        }
    }`,
			func() query.Query {
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
