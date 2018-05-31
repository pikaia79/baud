package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
)

type IdsQuery struct {
	query.Query     `json:"-"`

}

func NewIdsQuery() *IdsQuery {
	return &IdsQuery{}
}

func (i *IdsQuery) SetQuery(query query.Query) {
	i.Query = query
}

func (i *IdsQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Type  string    `json:"type,omitempty"`
		Ids   []string  `json:"values"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q := query.NewDocIDQuery(tmp.Ids)
	i.Query = q
	return nil
}
