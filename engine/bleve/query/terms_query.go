package query

import (
	"encoding/json"

	"github.com/blevesearch/bleve/search/query"
)

type TermsQuery struct {
	query.Query
}

func NewTermsQuery() *TermsQuery {
	return &TermsQuery{}
}

func (ts *TermsQuery) UnmarshalJSON(data []byte) error {
	tmp := make(map[string][]string)
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	for field, terms := range tmp {
		q := query.NewBooleanQuery(nil, nil, nil)
		q.SetBoost(1.0)
		for _, term := range terms {
			sq := query.NewTermQuery(term)
			sq.SetBoost(1.0)
			sq.SetField(field)
			q.AddShould(sq)
		}
		q.SetMinShould(1.0)
		ts.Query = q
		break
	}
	return nil
}
