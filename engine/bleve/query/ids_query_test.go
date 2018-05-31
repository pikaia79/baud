package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"reflect"
)

func TestIdsQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{`{
        "type" : "my_type",
        "values" : ["1", "4", "100"]
    }`,
		func() query.Query {
			utq := query.NewDocIDQuery([]string{"1", "4", "100"})
			utq.SetBoost(1.0)
			q := NewIdsQuery()
			q.SetQuery(utq)
			return q
		}(),},
		QueryTestGroup{
			`{
        "user" : {
            "value" :         "ki",
            "boost" :         1.0,
            "fuzziness" :     2,
            "prefix_length" : 0,
            "max_expansions": 100
        }
    }`,
			func() query.Query {
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
