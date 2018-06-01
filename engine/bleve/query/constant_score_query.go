package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
)

type ConstantScoreQuery struct {
	query.Query
	BoostVal *Boost
}

func NewConstantScoreQuery() *ConstantScoreQuery{
	return &ConstantScoreQuery{}
}

func (c *ConstantScoreQuery)SetBoost(boost float64) {
	b := Boost(boost)
	c.BoostVal = &b
}

func (c *ConstantScoreQuery)SetQuery(query query.Query) {
	c.Query = query
}

func (c *ConstantScoreQuery)UnmarshalJSON(data []byte) error {
	tmp := struct {
		Filter  json.RawMessage `json:"filter"`
		Boost   *Boost          `json:"boost,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q, err := ParseQuery(tmp.Filter)
	if err != nil {
		return err
	}
	c.Query = q
	c.BoostVal = tmp.Boost
	return nil
}
