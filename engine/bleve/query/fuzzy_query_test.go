package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
)

func TestFuzzyQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{input:`{ "user" : "ki" }`,
		output: func() query.Query {
			utq := query.NewFuzzyQuery("ki")
			utq.SetField("user")
			utq.SetBoost(1.0)
			q := NewFuzzyQuery()
			q.SetQuery(utq)
			return q
		}(),},
		QueryTestGroup{
			input: `{
        "user" : {
            "value" :         "ki",
            "boost" :         1.0,
            "fuzziness" :     2,
            "prefix_length" : 0,
            "max_expansions": 100
        }
    }`,
			output: func() query.Query {
				utq := query.NewFuzzyQuery("ki")
				utq.SetField("user")
				utq.SetBoost(1.0)
				utq.SetPrefix(0)
				utq.SetFuzziness(2)
				q := NewFuzzyQuery()
				q.SetMaxExpansions(100)
				q.SetQuery(utq)
				return q
			}(),},

	}

	for _, group := range groups {
		tq := NewFuzzyQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
