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
	"testing"
)

func TestPlanValueIsNull(t *testing.T) {
	tcases := []struct {
		in  PlanValue
		out bool
	}{{
		in:  PlanValue{},
		out: true,
	}, {
		in:  PlanValue{Key: "aa"},
		out: false,
	}, {
		in:  PlanValue{Value: NewVarBinary("aa")},
		out: false,
	}, {
		in:  PlanValue{ListKey: "aa"},
		out: false,
	}, {
		in:  PlanValue{Values: []PlanValue{}},
		out: false,
	}}
	for _, tc := range tcases {
		got := tc.in.IsNull()
		if got != tc.out {
			t.Errorf("IsNull(%v): %v, want %v", tc.in, got, tc.out)
		}
	}
}

func TestPlanValueIsList(t *testing.T) {
	tcases := []struct {
		in  PlanValue
		out bool
	}{{
		in:  PlanValue{},
		out: false,
	}, {
		in:  PlanValue{Key: "aa"},
		out: false,
	}, {
		in:  PlanValue{Value: NewVarBinary("aa")},
		out: false,
	}, {
		in:  PlanValue{ListKey: "aa"},
		out: true,
	}, {
		in:  PlanValue{Values: []PlanValue{}},
		out: true,
	}}
	for _, tc := range tcases {
		got := tc.in.IsList()
		if got != tc.out {
			t.Errorf("IsList(%v): %v, want %v", tc.in, got, tc.out)
		}
	}
}
