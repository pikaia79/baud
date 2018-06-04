package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
	"errors"
)

type FilteredQuery struct {
	query.Query
}

func NewFilteredQuery() *FilteredQuery {
	return &FilteredQuery{}
}

// the filtered query is replaced by the bool query.
func (f *FilteredQuery)UnmarshalJSON(data []byte) error {
	tmp := struct {
		Query    json.RawMessage    `json:"query,omitempty"`
		Filter   []json.RawMessage    `json:"filter,omitempty"`
		Boost    *Boost              `json:"boost,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var must []query.Query
	if tmp.Query != nil {
		q, err := ParseQuery(tmp.Query)
		if err != nil {
			return err
		}
		must = append(must, q)
	}
	if tmp.Filter == nil {
		return errors.New("invalid filtered")
	}
	for _, filter := range tmp.Filter {
		q, err := ParseQuery(filter)
		if err != nil {
			return err
		}
		must = append(must, q)
	}
	q := query.NewBooleanQuery(must, nil, nil)
	if tmp.Boost != nil {
		q.SetBoost(tmp.Boost.Value())
	}
	return nil
}
