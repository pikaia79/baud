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
	"reflect"
	"testing"

	querypb "github.com/tiglabs/baudengine/proto/mysqlpb"
)

func TestRepair(t *testing.T) {
	fields := []*querypb.Field{{
		Type: Int64,
	}, {
		Type: VarChar,
	}}
	in := Result{
		Rows: [][]Value{
			{newTestValue(VarBinary, "1"), newTestValue(VarBinary, "aa")},
			{newTestValue(VarBinary, "2"), newTestValue(VarBinary, "bb")},
		},
	}
	want := Result{
		Rows: [][]Value{
			{newTestValue(Int64, "1"), newTestValue(VarChar, "aa")},
			{newTestValue(Int64, "2"), newTestValue(VarChar, "bb")},
		},
	}
	in.Repair(fields)
	if !reflect.DeepEqual(in, want) {
		t.Errorf("Repair:\n%#v, want\n%#v", in, want)
	}
}

func TestCopy(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{newTestValue(Int64, "1"), MakeTrusted(Null, nil)},
			{newTestValue(Int64, "2"), MakeTrusted(VarChar, nil)},
			{newTestValue(Int64, "3"), newTestValue(VarChar, "")},
		},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "sh",
				Position:  "po",
			},
			Fresher: true,
		},
	}
	out := in.Copy()
	if !reflect.DeepEqual(out, in) {
		t.Errorf("Copy:\n%v, want\n%v", out, in)
	}
}

func TestTruncate(t *testing.T) {
	in := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}, {
			Type: VarChar,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{newTestValue(Int64, "1"), MakeTrusted(Null, nil)},
			{newTestValue(Int64, "2"), MakeTrusted(VarChar, nil)},
			{newTestValue(Int64, "3"), newTestValue(VarChar, "")},
		},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "sh",
				Position:  "po",
			},
			Fresher: true,
		},
	}

	out := in.Truncate(0)
	if !reflect.DeepEqual(out, in) {
		t.Errorf("Truncate(0):\n%v, want\n%v", out, in)
	}

	out = in.Truncate(1)
	want := &Result{
		Fields: []*querypb.Field{{
			Type: Int64,
		}},
		InsertID:     1,
		RowsAffected: 2,
		Rows: [][]Value{
			{newTestValue(Int64, "1")},
			{newTestValue(Int64, "2")},
			{newTestValue(Int64, "3")},
		},
		Extras: &querypb.ResultExtras{
			EventToken: &querypb.EventToken{
				Timestamp: 123,
				Shard:     "sh",
				Position:  "po",
			},
			Fresher: true,
		},
	}
	if !reflect.DeepEqual(out, want) {
		t.Errorf("Truncate(1):\n%v, want\n%v", out, want)
	}
}
