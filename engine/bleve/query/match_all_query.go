package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
)

type MatchAllQuery struct {
	query.Query
}

func NewMatchAllQuery() *MatchAllQuery {
	return &MatchAllQuery{}
}

func (m *MatchAllQuery) SetQuery(query query.Query) {
	m.Query = query
}

func (m *MatchAllQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Boost   *Boost          `json:"boost,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q := query.NewMatchAllQuery()
	if tmp.Boost != nil {
		q.SetBoost(tmp.Boost.Value())
	}
	m.Query = q
	return nil
}