/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package errors

import (
	"errors"
	"testing"

	"golang.org/x/net/context"

	codepb "github.com/tiglabs/baudengine/proto/mysqlpb"
)

func TestCreation(t *testing.T) {
	testcases := []struct {
		in, want codepb.Code
	}{{
		in:   codepb.Code_CANCELED,
		want: codepb.Code_CANCELED,
	}, {
		in:   codepb.Code_UNKNOWN,
		want: codepb.Code_UNKNOWN,
	}}
	for _, tcase := range testcases {
		if got := Code(New(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(New(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
		if got := Code(Errorf(tcase.in, "")); got != tcase.want {
			t.Errorf("Code(Errorf(%v)): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestCode(t *testing.T) {
	testcases := []struct {
		in   error
		want codepb.Code
	}{{
		in:   nil,
		want: codepb.Code_OK,
	}, {
		in:   errors.New("generic"),
		want: codepb.Code_UNKNOWN,
	}, {
		in:   New(codepb.Code_CANCELED, "generic"),
		want: codepb.Code_CANCELED,
	}, {
		in:   context.Canceled,
		want: codepb.Code_CANCELED,
	}, {
		in:   context.DeadlineExceeded,
		want: codepb.Code_DEADLINE_EXCEEDED,
	}}
	for _, tcase := range testcases {
		if got := Code(tcase.in); got != tcase.want {
			t.Errorf("Code(%v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
