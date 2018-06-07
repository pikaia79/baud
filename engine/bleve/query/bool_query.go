package query

import (
	"encoding/json"
	"bytes"
	"strconv"
	"github.com/blevesearch/bleve/search/query"

)

type BoolQuery struct {
	query.Query
}

func NewBoolQuery() *BoolQuery {
	return &BoolQuery{}
}

func (b *BoolQuery) SetQuery(query query.Query) {
	b.Query = query
}

func (b *BoolQuery)UnmarshalJSON(data []byte) error {
	tmp := struct {
		Must    []json.RawMessage      `json:"must,omitempty"`
		MustNot []json.RawMessage      `json:"must_not,omitempty"`
		Filter  []json.RawMessage      `json:"filter,omitempty"`
		Should  []json.RawMessage      `json:"should,omitempty"`
		Boost   *Boost               `json:"boost,omitempty"`
		MinimumShouldMatch json.RawMessage `json:"minimum_should_match,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var must, must_not, should []query.Query
	for _, _must := range tmp.Must {
		q, err := ParseQuery(_must)
		if err != nil {
			return err
		}
		must = append(must, q)
	}
	for _, _must_not := range tmp.MustNot {
		q, err := ParseQuery(_must_not)
		if err != nil {
			return err
		}
		must_not = append(must_not, q)
	}
	for _, _filter := range tmp.Filter {
		q, err := ParseQuery(_filter)
		if err != nil {
			return err
		}
		must = append(must, q)
	}
	for _, _should := range tmp.Should {
		q, err := ParseQuery(_should)
		if err != nil {
			return err
		}
		should = append(should, q)
	}
	q := query.NewBooleanQuery(must, should, must_not)
	if tmp.Boost != nil {
		q.SetBoost(tmp.Boost.Value())
	}
	if tmp.MinimumShouldMatch != nil {
		min, err := ParseMinShould([]byte(tmp.MinimumShouldMatch), len(should))
		if err != nil {
			return err
		}
		q.SetMinShould(min)
	}
	b.Query = q
	return nil
}

/*
    Integer                 3
    Negative integer        -2
    Percentage              75%
    Negative percentage     -25%
    Combination             3<90%
    Multiple combinations   2<-25% 9<-3 [baudengine not support now]
*/
func parseMinimumShouldMatch(data []byte, maxShould int) (min float64, err error) {
	if bytes.ContainsAny(data, "<") {
		ms := bytes.Split(data, []byte{'<'})
		if len(ms) != 2 {
			err = ErrInvalidMinMatch
			return
		}
		var m1, m2 float64
		m1, err = parseMinimumShouldMatch(ms[0], maxShould)
		if err != nil {
			return 0.0, err
		}
		m2, err = parseMinimumShouldMatch(ms[1], maxShould)
		if err != nil {
			return 0.0, err
		}
		if maxShould <= int(m1) {
			if m2 < m1 {
				min = m2
			} else {
				min = m1
			}
		} else {
			min = m2
		}
		return
	} else if bytes.ContainsAny(data, "%") {
		min, err = strconv.ParseFloat(string(data[:len(data)-1]), 64)
		if err != nil {
			return
		}
		if min < -100 || min > 100 {
			return 0.0, ErrInvalidMinMatch
		}
		min = min*0.01*float64(maxShould)
		if min < 0 {
			min = float64(maxShould + int(min))
		}
		return min, nil
	} else {
		min, err = strconv.ParseFloat(string(data), 64)
		if err != nil {
			return
		}
		if min < 0 {
			min = float64(maxShould) + min
		}
	}

	return
}

func ParseMinShould(data []byte, maxShould int) (float64, error) {
	min, err := parseMinimumShouldMatch(data, maxShould)
	if err != nil {
		return 0.0, err
	}
	min = float64(int(min))
	return min, nil
}
