/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"encoding/json"
)

// PlanValue represents a value or a list of values for
// a column that will later be resolved using bind vars and used
// to perform plan actions like generating the final query or
// deciding on a route.
//
// Plan values are typically used as a slice ([]planValue)
// where each entry is for one column. For situations where
// the required output is a list of rows (like in the case
// of multi-value inserts), the representation is pivoted.
// For example, a statement like this:
// 	INSERT INTO t VALUES (1, 2), (3, 4)
// will be represented as follows:
// 	[]PlanValue{
// 		Values: {1, 3},
// 		Values: {2, 4},
// 	}
//
// For WHERE clause items that contain a combination of
// equality expressions and IN clauses like this:
//   WHERE pk1 = 1 AND pk2 IN (2, 3, 4)
// The plan values will be represented as follows:
// 	[]PlanValue{
// 		Value: 1,
// 		Values: {2, 3, 4},
// 	}
// When converted into rows, columns with single values
// are replicated as the same for all rows:
// 	[][]Value{
// 		{1, 2},
// 		{1, 3},
// 		{1, 4},
// 	}
type PlanValue struct {
	Key     string
	Value   Value
	ListKey string
	Values  []PlanValue
}

// IsNull returns true if the PlanValue is NULL.
func (pv PlanValue) IsNull() bool {
	return pv.Key == "" && pv.Value.IsNull() && pv.ListKey == "" && pv.Values == nil
}

// IsList returns true if the PlanValue is a list.
func (pv PlanValue) IsList() bool {
	return pv.ListKey != "" || pv.Values != nil
}

// MarshalJSON should be used only for testing.
func (pv PlanValue) MarshalJSON() ([]byte, error) {
	switch {
	case pv.Key != "":
		return json.Marshal(":" + pv.Key)
	case !pv.Value.IsNull():
		if pv.Value.IsIntegral() {
			return pv.Value.ToBytes(), nil
		}
		return json.Marshal(pv.Value.ToString())
	case pv.ListKey != "":
		return json.Marshal("::" + pv.ListKey)
	case pv.Values != nil:
		return json.Marshal(pv.Values)
	}
	return []byte("null"), nil
}
