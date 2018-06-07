package query

import (
	"testing"
	"reflect"
	"encoding/json"

	"github.com/blevesearch/bleve/search/query"
)

func TestMatchQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{input:`{
        "message" : "this is a test"
    }`,
		output:func() query.Query {
			utq := query.NewMatchQuery("this is a test")
			utq.SetField("message")
			q := NewMatchQuery()
			q.SetQuery(utq)
			return q
		}(),},

		QueryTestGroup{input:`{"message" : {
            "query" : "this is a test",
            "type" : "phrase"
            }
    }`,
			output: func() query.Query {
				utq := query.NewMatchPhraseQuery("this is a test")
				utq.SetField("message")
				q := NewMatchQuery()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{input:`{
        "message" : {
            "query" : "this is a test",
            "analyzer" : "my_analyzer"
        }
    }`,
			output: func() query.Query {
				utq := query.NewMatchQuery("this is a test")
				utq.SetField("message")
				q := NewMatchQuery()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{input:`{
        "message" : {
            "query" : "this is a test",
            "operator" : "and"
        }
    }`,
			output: func() query.Query {
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
