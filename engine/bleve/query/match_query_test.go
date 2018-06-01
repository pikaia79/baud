package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
)

func TestMatchQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{`{
        "message" : "this is a test"
    }`,
		func() query.Query {
			utq := query.NewMatchQuery("this is a test")
			utq.SetField("message")
			q := NewMatchQuery()
			q.SetQuery(utq)
			return q
		}(),},

		QueryTestGroup{`{"message" : {
            "query" : "this is a test",
            "type" : "phrase"
            }
    }`,
			func() query.Query {
				utq := query.NewMatchPhraseQuery("this is a test")
				utq.SetField("message")
				q := NewMatchQuery()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{`{
        "message" : {
            "query" : "this is a test",
            "analyzer" : "my_analyzer"
        }
    }`,
			func() query.Query {
				utq := query.NewMatchQuery("this is a test")
				utq.SetField("message")
				q := NewMatchQuery()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{`{
        "message" : {
            "query" : "this is a test",
            "operator" : "and"
        }
    }`,
			func() query.Query {
				utq := query.NewMatchQuery("this is a test")
				utq.SetField("message")
				utq.SetOperator(query.MatchQueryOperatorAnd)
				q := NewMatchQuery()
				q.SetQuery(utq)
				return q
			}(),},
	}

	for _, group := range groups {
		tq := NewMatchQuery()
		err := json.Unmarshal([]byte(group.input), tq)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tq, group.output) {
			t.Fatalf("parse failed %v %v", tq, group.output)
		}
	}
}
