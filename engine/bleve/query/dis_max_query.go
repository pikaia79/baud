package query

import (
	"encoding/json"
	"errors"

	"github.com/blevesearch/bleve/search/query"
)

type DisMaxQuery struct {
	query.Query
}

func NewDisMaxQuery() *DisMaxQuery {
	return &DisMaxQuery{}
}

func (d *DisMaxQuery) SetQuery(query query.Query) {
	d.Query = query
}

func (d *DisMaxQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Queries    []json.RawMessage    `json:"queries"`
		Boost      *Boost               `json:"boost,omitempty"`
		TieBreaker *float64             `json:"tie_breaker,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var should []query.Query
	for _, query := range tmp.Queries {
		q, err := ParseQuery([]byte(query))
		if err != nil {
			return err
		}
		should = append(should, q)
	}
	if should == nil {
		return errors.New("invalid dis max query")
	}
	q := query.NewBooleanQuery(nil, should, nil)
	if tmp.Boost != nil {
		q.SetBoost(tmp.Boost.Value())
	}
	d.Query = q
	return nil
}