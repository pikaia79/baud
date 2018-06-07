package query

import (
	"reflect"
	"encoding/json"
	"errors"

	"github.com/blevesearch/bleve/search/query"
)

type WildcardQuery struct {
	query.Query
}

func NewWildcardQuery() *WildcardQuery {
	return &WildcardQuery{}
}

/*
{ "user" : "ki*y" }
{ "user" : { "value" : "ki*y", "boost" : 2.0 } }
{ "user" : { "wildcard" : "ki*y", "boost" : 2.0 } }
*/
func (w *WildcardQuery)UnmarshalJSON(data []byte) error {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var tt *query.WildcardQuery
	for field, tq := range tmp {
		val := reflect.ValueOf(tq)
		typ := val.Type()
		switch typ.Kind() {
		case reflect.String:
			tt = query.NewWildcardQuery(val.String())
			tt.SetBoost(1.0)
			tt.SetField(field)
		case reflect.Map:
			var prefix string
			var boost float64 = 1.0
			for _, key := range val.MapKeys() {
				if key.String() == "value" {
					prefix = reflect.ValueOf(val.MapIndex(key).Interface()).String()
				} else if key.String() == "wildcard" {
					prefix = reflect.ValueOf(val.MapIndex(key).Interface()).String()
				} else if key.String() == "boost" {
					boost, err = toFloat(val.MapIndex(key).Interface())
				} else {
					return errors.New("invalid wildcard query")
				}
			}
			tt = query.NewWildcardQuery(prefix)
			tt.SetBoost(boost)
			tt.SetField(field)
		default:
			return errors.New("invalid wildcard query")
		}
		if tt == nil {
			return errors.New("invalid wildcard query")
		}
		w.Query = tt
		return nil
	}
	return nil
}
