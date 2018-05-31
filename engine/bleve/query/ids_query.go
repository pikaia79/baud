package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
)

type IdsQuery struct {
	query.Query     `json:"-"`
	Type  string    `json:"type,omitempty"`
	Ids   []string  `json:"values"`
}

func NewIdsQuery() *IdsQuery {
	return &IdsQuery{}
}

func (i *IdsQuery) SetQuery(query query.Query) {
	i.Query = query
}

func (i *IdsQuery) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, i)
	if err != nil {
		return err
	}
	q := query.NewDocIDQuery(i.Ids)
	i.Query = q
	return nil
}
