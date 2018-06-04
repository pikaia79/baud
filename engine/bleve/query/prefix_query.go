package query

import (
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
	"errors"
)

type PrefixQuery struct {
	query.Query
}

func NewPrefixQuery() *PrefixQuery {
	return &PrefixQuery{}
}

/*
{ "user" : "ki" }

{ "user" :  { "value" : "ki", "boost" : 2.0 } }
{ "user" :  { "prefix" : "ki", "boost" : 2.0 } }
*/
func (f *PrefixQuery) UnmarshalJSON(data []byte) error {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var tt *query.PrefixQuery
	for field, tq := range tmp {
		val := reflect.ValueOf(tq)
		typ := val.Type()
		switch typ.Kind() {
		case reflect.String:
			tt = query.NewPrefixQuery(val.String())
			tt.SetBoost(1.0)
			tt.SetField(field)
		case reflect.Map:
			var prefix string
			var boost float64
			for _, key := range val.MapKeys() {
				if key.String() == "value" {
					prefix = reflect.ValueOf(val.MapIndex(key).Interface()).String()
				} else if key.String() == "prefix" {
					prefix = reflect.ValueOf(val.MapIndex(key).Interface()).String()
				} else if key.String() == "boost" {
					boost, err = toFloat(val.MapIndex(key).Interface())
					if err != nil {
						return err
					}
				} else {
					return errors.New("invalid prefix query")
				}
			}
			tt = query.NewPrefixQuery(prefix)
			tt.SetBoost(boost)
			tt.SetField(field)
		default:
			return errors.New("invalid term query")
		}
		if tt == nil {
			return errors.New("invalid term query")
		}
		f.Query = tt
		return nil
	}
	return nil
}
