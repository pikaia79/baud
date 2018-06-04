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
	"fmt"

	"golang.org/x/net/context"

	codepb "github.com/tiglabs/baudengine/proto/mysqlpb"
)

type mysqlError struct {
	code codepb.Code
	err  string
}

// New creates a new error using the code and input string.
func New(code codepb.Code, in string) error {
	if code == codepb.Code_OK {
		panic("OK is an invalid error code; use INTERNAL instead")
	}
	return &mysqlError{
		code: code,
		err:  in,
	}
}

// Wrap wraps the given error, returning a new error with the given message as a prefix but with the same error code (if err was a vterror) and message of the passed error.
func Wrap(err error, message string) error {
	return New(Code(err), fmt.Sprintf("%v: %v", message, err.Error()))
}

// Wrapf wraps the given error, returning a new error with the given format string as a prefix but with the same error code (if err was a vterror) and message of the passed error.
func Wrapf(err error, format string, args ...interface{}) error {
	return Wrap(err, fmt.Sprintf(format, args...))
}

// Errorf returns a new error built using Printf style arguments.
func Errorf(code codepb.Code, format string, args ...interface{}) error {
	return New(code, fmt.Sprintf(format, args...))
}

func (e *mysqlError) Error() string {
	return e.err
}

// Code returns the error code if it's a mysqlError.
// If err is nil, it returns ok. Otherwise, it returns unknown.
func Code(err error) codepb.Code {
	if err == nil {
		return codepb.Code_OK
	}
	if err, ok := err.(*mysqlError); ok {
		return err.code
	}
	// Handle some special cases.
	switch err {
	case context.Canceled:
		return codepb.Code_CANCELED
	case context.DeadlineExceeded:
		return codepb.Code_DEADLINE_EXCEEDED
	}
	return codepb.Code_UNKNOWN
}

// Equals returns true iff the error message and the code returned by Code()
// is equal.
func Equals(a, b error) bool {
	if a == nil && b == nil {
		// Both are nil.
		return true
	}

	if a == nil && b != nil || a != nil && b == nil {
		// One of the two is nil.
		return false
	}

	return a.Error() == b.Error() && Code(a) == Code(b)
}

// Print is meant to print the mysqlError object in test failures.
// For comparing two vterrors, use Equals() instead.
func Print(err error) string {
	return fmt.Sprintf("%v: %v", Code(err), err.Error())
}
