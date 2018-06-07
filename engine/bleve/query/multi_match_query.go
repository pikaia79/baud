package query

import (
	"encoding/json"
	"regexp"
	"errors"
	"strconv"

	"github.com/blevesearch/bleve/search/query"
)

var boostReg = regexp.MustCompile(`\^`)
var wildcardReg = regexp.MustCompile(`\*|\?`)

type MultiMatch struct {
	query.Query

}

func NewMultiMatch() *MultiMatch {
	return &MultiMatch{}
}

func (m *MultiMatch)SetQuery(query query.Query) {
	m.Query = query
}

func (m *MultiMatch)UnmarshalJSON(data []byte) error {
	tmp := struct {
		Query    string       `json:"query"`
		Type     *string      `json:"type,omitempty"`
		Fields   []string     `json:"fields"`
		TieBreaker *float64   `json:"tie_breaker,omitempty"`
		Operator  *string     `json:"operator,omitempty"`
		Analyzer  *string     `json:"analyzer,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var querys []query.Query
	for _, filed := range tmp.Fields {
		list := boostReg.Split(filed, -1)
		num := len(list)
		if num == 0 || num > 2 {
			return errors.New("invalid query")
		}
		var fieldName string
		var boost *Boost
		for i, l := range list {
			if i == 0 {
				fieldName = l
			} else {
				b, err := strconv.ParseFloat(l, 64)
				if err != nil {
					return err
				}
				boost = NewBoost(b)
			}
			if wildcardReg.MatchString(fieldName) {
				return errors.New("not support wildcard field")
			}
		}
		q := query.NewMatchQuery(tmp.Query)
		q.SetField(fieldName)
		if boost != nil {
			q.SetBoost(boost.Value())
		}
		if tmp.Analyzer != nil {
			q.Analyzer = *tmp.Analyzer
		}
        if tmp.Operator != nil {
	        switch *tmp.Operator {
	        case "and":
	        	q.SetOperator(query.MatchQueryOperatorAnd)
	        case "or":
	        	q.SetOperator(query.MatchQueryOperatorOr)
	        default:
				return errors.New("invalid operator")
	        }
        }
        querys = append(querys, q)
	}
	// TODO type
	m.SetQuery(query.NewBooleanQuery(nil, querys, nil))
	return nil
}
