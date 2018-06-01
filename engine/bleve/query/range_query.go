package query

import (
	"github.com/blevesearch/bleve/search/query"
	"encoding/json"
	"errors"
)

type RangeQuery struct {
	query.Query
}

func NewRangeQuery() *RangeQuery {
	return &RangeQuery{}
}

/*
{
    "range" : {
        "age" : {
            "gte" : 10,
            "lte" : 20,
            "boost" : 2.0
        }
    }
}

{
    "range" : {
        "date" : {
            "gte" : "now-1d/d",
            "lt" :  "now/d"
        }
    }
}
*/
func (r *RangeQuery)UnmarshalJSON(data []byte) error {
	tmp := make(map[string]map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
    for field, rv := range tmp {
    	var max, min *string
    	var maxNum, minNum *float64
    	var minInclusive, maxInclusive bool
	    var hasGte,hasGt,hasLte,hasLt bool
	    _, hasGte = rv["gte"]
	    _, hasGt = rv["gt"]
	    _, hasLte = rv["lte"]
	    _, hasLt = rv["lt"]
	    if (hasGte && hasGt) || (hasLt && hasLte){
		    return errors.New("invalid range query")
	    }
	    boost := 1.0
    	for key, v := range rv {
    		switch key {
		    case "gte":
		    	minInclusive = true
		    	if vv, ok := v.(string); ok {
		    		min = &vv
			    } else if vv, ok := v.(float64); ok {
			    	minNum = &vv
			    } else {
			    	return errors.New("invalid range query")
			    }
		    case "gt":
			    if vv, ok := v.(string); ok {
				    min = &vv
			    } else if vv, ok := v.(float64); ok {
				    minNum = &vv
			    } else {
				    return errors.New("invalid range query")
			    }
		    case "lte":
		    	maxInclusive = true
			    if vv, ok := v.(string); ok {
				    max = &vv
			    } else if vv, ok := v.(float64); ok {
				    maxNum = &vv
			    } else {
				    return errors.New("invalid range query")
			    }
		    case "lt":
			    if vv, ok := v.(string); ok {
				    max = &vv
			    } else if vv, ok := v.(float64); ok {
				    maxNum = &vv
			    } else {
				    return errors.New("invalid range query")
			    }
		    case "boost":
		    	boost, err = toFloat(v)
		    	if err != nil {
		    		return err
			    }
		    case "format":
		    	// todo time
		    case "time_zone":
		    	// todo time
		    }
	    }
	    if (minNum != nil || maxNum != nil) && min == nil && max == nil {
	    	q := query.NewNumericRangeInclusiveQuery(minNum, maxNum, &minInclusive, &maxInclusive)
	    	q.SetField(field)
	    	q.SetBoost(boost)
	    	r.Query = q
	    	return nil
	    } else {
		    q := query.NewTermRangeInclusiveQuery(*min, *max, &minInclusive, &maxInclusive)
		    q.SetField(field)
		    q.SetBoost(boost)
		    r.Query = q
		    return nil
	    }
    }
    return nil
}
