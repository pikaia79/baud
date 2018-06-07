package query

import (
	"testing"
	"reflect"
	"encoding/json"

	"github.com/blevesearch/bleve/search/query"
)

func TestDisMaxQuery(t *testing.T) {
	groups := []QueryTestGroup{
		QueryTestGroup{
			input: `{
            "queries": [
                { "match": { "title": "Quick pets" }},
                { "match": { "body":  "Quick pets" }}
            ],
            "tie_breaker": 0.3
        }`,
        output: func() *DisMaxQuery {
        	qm1 := query.NewMatchQuery("Quick pets")
        	qm1.SetField("title")
        	qm11 := NewMatchQuery()
        	qm11.SetQuery(qm1)
        	qm2 := query.NewMatchQuery("Quick pets")
        	qm2.SetField("body")
	        qm12 := NewMatchQuery()
	        qm12.SetQuery(qm2)
        	q := query.NewBooleanQuery(nil, []query.Query{qm11, qm12}, nil)
        	qq := NewDisMaxQuery()
        	qq.SetQuery(q)
        	return qq
        }(),
		},
	}

	for _, group := range groups {
		tq := NewDisMaxQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
