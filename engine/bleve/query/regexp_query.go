package query

import (
	"github.com/blevesearch/bleve/search/query"
	"reflect"
	"encoding/json"
	"errors"
)

type RegexpQuery struct {
	query.Query
	// bleve no flags
	flags  string
	// bleve no states
	max_determinized_states int
}

func NewRegexpQuery() *RegexpQuery {
	return &RegexpQuery{flags:"ALL", max_determinized_states: 10000}
}

func (r *RegexpQuery)SetFlags(flags string) {
	r.flags = flags
}

func (r *RegexpQuery)SetMaxDeterminizedStates(states int) {
	r.max_determinized_states = states
}

func (r *RegexpQuery) SetQuery(query query.Query) {
	r.Query = query
}

/*
{
        "name.first": "s.*y"
    }
{
        "name.first":{
            "value":"s.*y",
            "boost":1.2
        }
    }

{
        "name.first": {
            "value": "s.*y",
            "flags" : "INTERSECTION|COMPLEMENT|EMPTY"
        }
    }

{
        "name.first": {
            "value": "s.*y",
            "flags" : "INTERSECTION|COMPLEMENT|EMPTY",
            "max_determinized_states": 20000
        }
    }


*/
func (r *RegexpQuery) UnmarshalJSON(data []byte) error {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	var tt *query.RegexpQuery
	for field, tq := range tmp {
		val := reflect.ValueOf(tq)
		typ := val.Type()
		switch typ.Kind() {
		case reflect.String:
			tt = query.NewRegexpQuery(val.String())
			tt.SetBoost(1.0)
			tt.SetField(field)
		case reflect.Map:
			var regexp string
			var flags string
			var boost float64 = 1.0
			var states int64 = 10000
			for _, key := range val.MapKeys() {
				if key.String() == "value" {
					regexp = reflect.ValueOf(val.MapIndex(key).Interface()).String()
				} else if key.String() == "flags" {
					flags = reflect.ValueOf(val.MapIndex(key).Interface()).String()
					r.SetFlags(flags)
				} else if key.String() == "boost" {
					boost, err = toFloat(val.MapIndex(key).Interface())
					if err != nil {
						return err
					}
				} else if key.String() == "max_determinized_states" {
						states, err = toInt(val.MapIndex(key).Interface())
						if err != nil {
							return err
						}
						r.SetMaxDeterminizedStates(int(states))
				}else {
					return errors.New("invalid regexp query")
				}
			}
			tt = query.NewRegexpQuery(regexp)
			tt.SetBoost(boost)
			tt.SetField(field)
		default:
			return errors.New("invalid regexp query")
		}
		if tt == nil {
			return errors.New("invalid regexp query")
		}
		r.Query = tt
		return nil
	}
	return nil
}
