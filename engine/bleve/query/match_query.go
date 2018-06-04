package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
	"reflect"
	"fmt"
	"errors"
)

type MatchQuery struct {
	query.Query
}

func NewMatchQuery() *MatchQuery {
	return &MatchQuery{}
}

func (m *MatchQuery)SetQuery(query query.Query) {
	m.Query = query
}

func (m *MatchQuery)unmarshalJSON(data []byte) error {
	tmp := make(map[string]struct{
		Query     string   `json:"query"`
		Type      string   `json:"type,omitempty"`
		Operator  string   `json:"operator,omitempty"`
		MaxExpansions *int `json:"max_expansions,omitempty"`
		CutoffFrequency *float64 `json:"cutoff_frequency,omitempty"`
		ZeroTermsQuery string `json:"zero_terms_query,omitempty"`
		PrefixLength *int `json:"prefix_length,omitempty"`
		Fuzziness *int `json:"fuzziness,omitempty"`
		Analyzer string `json:"analyzer,omitempty"`
		Boost    *Boost `json:"boost,omitempty"`
	})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	for field, fv := range tmp {
		switch fv.Type {
		case "phrase_prefix":
			return errors.New("not support phrase_prefix match")
		case "phrase":
			q := query.NewMatchPhraseQuery(fv.Query)
			q.SetField(field)
			if fv.Boost != nil {
				q.SetBoost(fv.Boost.Value())
			}
			m.Query = q
			return nil
		case "boolean", "":
			q := query.NewMatchQuery(fv.Query)
			if fv.Boost != nil {
				q.SetBoost(fv.Boost.Value())
			}
			q.SetField(field)
			if fv.Fuzziness != nil {
				q.SetFuzziness(*fv.Fuzziness)
			}
			if fv.PrefixLength != nil {
				q.SetPrefix(*fv.PrefixLength)
			}
			if fv.Operator != "" {
				switch fv.Operator {
				case "and":
				    q.SetOperator(query.MatchQueryOperatorAnd)
				case "or":
					q.SetOperator(query.MatchQueryOperatorOr)
				default:
					return fmt.Errorf("invalid operator %s", fv.Operator)
				}
			}
			m.Query = q
			return nil
		default:
			return fmt.Errorf("invalid match type %s", fv.Type)
		}
	}
	return errors.New("invalid match query")
}

func (m *MatchQuery)UnmarshalJSON(data []byte) error {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var q *query.MatchQuery
	for field, fv := range tmp {
		val := reflect.ValueOf(fv)
		typ := val.Type()
		switch typ.Kind() {
		case reflect.String:
			q = query.NewMatchQuery(val.String())
			q.SetField(field)
			m.SetQuery(q)
			return nil
		case reflect.Map:
			return m.unmarshalJSON(data)
		default:
			return errors.New("invalid match query")
		}
	}
	return nil
}


