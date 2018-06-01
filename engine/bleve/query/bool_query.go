package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
)

type BoolQuery struct {
	query.Query
}

func NewBoolQuery() *BoolQuery {
	return &BoolQuery{}
}

func (b *BoolQuery) SetQuery(query query.Query) {
	b.Query = query
}

func (b *BoolQuery)UnmarshalJSON(data []byte) error {
	tmp := struct {
		Must    []json.RawMessage      `json:"must,omitempty"`
		MustNot []json.RawMessage      `json:"must_not,omitempty"`
		Filter  []json.RawMessage      `json:"filter,omitempty"`
		Should  []json.RawMessage      `json:"should,omitempty"`
		Boost   *Boost               `json:"boost,omitempty"`
		MinimumShouldMatch json.RawMessage `json:"minimum_should_match,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var must, must_not, should []query.Query
	for _, _must := range tmp.Must {
		q, err := ParseQuery(_must)
		if err != nil {
			return err
		}
		must = append(must, q)
	}
	for _, _must_not := range tmp.MustNot {
		q, err := ParseQuery(_must_not)
		if err != nil {
			return err
		}
		must_not = append(must_not, q)
	}
	for _, _filter := range tmp.Filter {
		q, err := ParseQuery(_filter)
		if err != nil {
			return err
		}
		must = append(must, q)
	}
	for _, _should := range tmp.Should {
		q, err := ParseQuery(_should)
		if err != nil {
			return err
		}
		should = append(should, q)
	}
	q := query.NewBooleanQuery(must, should, must_not)
	if tmp.Boost != nil {
		q.SetBoost(tmp.Boost.Value())
	}
	b.Query = q
	return nil
}
