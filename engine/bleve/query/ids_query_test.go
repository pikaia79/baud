package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
)

func TestIdsQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{`{
        "type" : "my_type",
        "values" : ["1", "4", "100"]
    }`,
		func() query.Query {
			utq := query.NewDocIDQuery([]string{"1", "4", "100"})
			q := NewIdsQuery()
			q.SetQuery(utq)
			return q
		}(),},
	}

	for _, group := range groups {
		tq := NewIdsQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
