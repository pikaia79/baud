package query

import (
	"reflect"
	"encoding/json"
	"github.com/blevesearch/bleve/search/query"
)

type FuzzyQuery struct {
	query.Query
	max_expansions int
}

func NewFuzzyQuery() *FuzzyQuery {
	return &FuzzyQuery{max_expansions: 50}
}

func (f *FuzzyQuery) SetMaxExpansions(max int) {
	f.max_expansions = max
}

func (f *FuzzyQuery) SetQuery(query query.Query) {
	f.Query = query
}

/*
{ "user" : "ki" }

{
        "user" : {
            "value" :         "ki",
            "boost" :         1.0,
            "fuzziness" :     2,
            "prefix_length" : 0,
            "max_expansions": 100
        }
    }
*/
func (f *FuzzyQuery) UnmarshalJSON(data []byte) error {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var tt *query.FuzzyQuery
	for field, tq := range tmp {
		val := reflect.ValueOf(tq)
		typ := val.Type()
		switch typ.Kind() {
		case reflect.String:
			tt = query.NewFuzzyQuery(val.String())
			tt.SetBoost(1.0)
			tt.SetField(field)
		case reflect.Map:
			var term string
			var boost float64 = 1.0
			var fuzzy int64 = 1
			var prefix *int64
			var max_expansions *int64
			for _, key := range val.MapKeys() {
				switch key.String() {
				case "value":
					term = reflect.ValueOf(val.MapIndex(key).Interface()).String()
				case "boost":
					boost, err = toFloat(val.MapIndex(key).Interface())
					if err != nil {
						return err
					}
				case "fuzziness":
					fuzzy, err = toInt(val.MapIndex(key).Interface())
					if err != nil {
						return err
					}
				case "prefix_length":
					_prefix, err := toInt(val.MapIndex(key).Interface())
					if err != nil {
						return err
					}
					prefix = &_prefix
				case "max_expansions":
					_max_expansions, err := toInt(val.MapIndex(key).Interface())
					if err != nil {
						return err
					}
					max_expansions = &_max_expansions
				}
			}
			tt = query.NewFuzzyQuery(term)
			tt.SetBoost(boost)
			tt.SetField(field)
			if prefix != nil {
				tt.SetPrefix(int(*prefix))
			}
			tt.SetFuzziness(int(fuzzy))
			if max_expansions != nil {
				f.max_expansions = int(*max_expansions)
			}
		default:
			return ErrInvalidTermQuery
		}
		if tt == nil {
			return ErrInvalidTermQuery
		}
		f.Query = tt
		return nil
	}
	return nil
}
