package query

import (
	"github.com/blevesearch/bleve/search/query"
	"github.com/tiglabs/baudengine/util/json"
)

func ParseQuery(data []byte) (query.Query, error) {
	tmp := make(map[string]interface{})
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return nil, err
	}
	_, hasFilter := tmp["filtered"]
	if hasFilter {

	}
	return nil, nil
}
