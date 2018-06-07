package query

import (
	"testing"
	"github.com/blevesearch/bleve/search/query"
	"errors"
	"reflect"
)

func TestMultiMatchQuery(t *testing.T) {
	groups := []QueryTestGroup{QueryTestGroup{input:`{
  "multi_match" : {
    "query":    "this is a test", 
    "fields": [ "subject", "message" ] 
  }
}`,
		output: func() query.Query {
			q1 := query.NewMatchQuery("this is a test")
			q1.SetField("subject")
			q2 := query.NewMatchQuery("this is a test")
			q2.SetField("message")
			utq := query.NewBooleanQuery(nil, []query.Query{q1, q2}, nil)
			q := NewMultiMatch()
			q.SetQuery(utq)
			return q
		}(),},

		QueryTestGroup{input:`{
  "multi_match" : {
    "query":    "Will Smith",
    "fields": [ "title", "*_name" ] 
  }
}`,
			err: errors.New("not support wildcard field")},

		QueryTestGroup{input:`{
  "multi_match" : {
    "query" : "this is a test",
    "fields" : [ "subject^3", "message" ] 
  }
}`,
			output: func() query.Query {
				q1 := query.NewMatchQuery("this is a test")
				q1.SetField("subject")
				q1.SetBoost(3)
				q2 := query.NewMatchQuery("this is a test")
				q2.SetField("message")
				utq := query.NewBooleanQuery(nil, []query.Query{q1, q2}, nil)
				q := NewMultiMatch()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{input:`{
  "multi_match" : {
    "query":      "Will Smith",
    "type":       "best_fields",
    "fields":     [ "first_name", "last_name" ],
    "operator":   "and" 
  }
}`,
			output:func() query.Query {
				q1 := query.NewMatchQuery("Will Smith")
				q1.SetField("first_name")
				q1.SetOperator(query.MatchQueryOperatorAnd)
				q2 := query.NewMatchQuery("Will Smith")
				q2.SetField("last_name")
				q2.SetOperator(query.MatchQueryOperatorAnd)
				utq := query.NewBooleanQuery(nil, []query.Query{q1, q2}, nil)
				q := NewMultiMatch()
				q.SetQuery(utq)
				return q
			}(),},

		QueryTestGroup{input:`{
  "multi_match" : {
    "query":      "Jon",
    "type":       "cross_fields",
    "analyzer":   "standard", 
    "fields":     [ "first", "last", "edge" ]
  }
}`,
			output:func() query.Query {
				q1 := query.NewMatchQuery("Jon")
				q1.SetField("first")
				q1.Analyzer = "standard"
				q2 := query.NewMatchQuery("Jon")
				q2.SetField("last")
				q2.Analyzer = "standard"
				q3 := query.NewMatchQuery("Jon")
				q3.SetField("edge")
				q3.Analyzer = "standard"
				utq := query.NewBooleanQuery(nil, []query.Query{q1, q2, q3}, nil)
				q := NewMultiMatch()
				q.SetQuery(utq)
				return q
			}(),},
	}
	for i, g := range groups {
		output, err:= ParseQuery([]byte(g.input))
		if err != nil {
			if g.err != nil && g.err.Error() == err.Error() {
				continue
			}
			t.Fatalf("parse failed %v", err)
		}
		if !reflect.DeepEqual(output, g.output) {
			t.Fatalf("parse failed %d", i)
		}
	}
}
