package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
)

func TestRangeQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{`{
        "age" : {
            "gte" : 10,
            "lte" : 20,
            "boost" : 2.0
        }
    }`,
		func() query.Query {
			min := float64(10)
			max := float64(20)
			incMin := true
			incMax := true
			utq := query.NewNumericRangeInclusiveQuery(&min, &max, &incMin, &incMax)
			utq.SetField("age")
			utq.SetBoost(2.0)
			return utq
		}(),},
		QueryTestGroup{
			`{
        "date" : {
            "gte" : "now-1d/d",
            "lt" :  "now/d"
        }
    }`,
			func() query.Query {
				incMin := true
				incMax := false
				utq := query.NewTermRangeInclusiveQuery("now-1d/d", "now/d", &incMin, &incMax)
				utq.SetField("date")
				utq.SetBoost(1.0)
				return utq
			}(),},
	}

	for _, group := range groups {
		tq := NewRangeQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq.Query, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
