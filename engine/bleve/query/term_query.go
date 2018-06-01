package query

import (
	"github.com/blevesearch/bleve/search/query"
	"github.com/tiglabs/baudengine/util/json"
	"reflect"
	"errors"
)

type TermQuery struct {
	query.Query
}

func NewTermQuery() *TermQuery {
	return &TermQuery{}
}

func (t *TermQuery) SetQuery(query query.Query) {
	t.Query = query
}

/*
{
          "term": {
            "status": {
              "value": "urgent",
              "boost": 2.0
            }
          }
        },
        {
          "term": {
            "status": "normal"
          }
        }
*/
func (t *TermQuery)UnmarshalJSON(data []byte) error {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var tt *query.TermQuery
	for field, tq := range tmp {
		val := reflect.ValueOf(tq)
		typ := val.Type()
		switch typ.Kind() {
		case reflect.String:
			tt = query.NewTermQuery(val.String())
			tt.SetBoost(1.0)
			tt.SetField(field)
		case reflect.Map:
			var term string
			var boost float64 = 1.0
			for _, key := range val.MapKeys() {
				if key.String() == "value" {
					term = reflect.ValueOf(val.MapIndex(key).Interface()).String()
				} else if key.String() == "boost" {
					boost, err = toFloat(val.MapIndex(key).Interface())
					if err != nil {
						return err
					}
				} else {
					return errors.New("invalid term query")
				}
			}
			tt = query.NewTermQuery(term)
			tt.SetBoost(boost)
			tt.SetField(field)
		default:
			return errors.New("invalid term query")
		}
		if tt == nil {
			return errors.New("invalid term query")
		}
		t.Query = tt
		return nil
	}
	return nil
}