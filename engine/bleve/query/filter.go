package query

import "github.com/blevesearch/bleve/search/query"

type Filtered struct {
	query.Query
	Filter string   `json:"filter"`
}

func (f *Filtered)UnmarshalJSON([]byte) error {
	return nil
}
