package numeric

import (
	"testing"
)

// test that the float/sortable int operations work both ways
// and that the corresponding integers sort the same as
// the original floats would have
func TestSortabledFloat64ToInt64(t *testing.T) {
	tests := []struct {
		input float64
	}{
		{
			input: -4640094584139352638,
		},
		{
			input: -167.42,
		},
		{
			input: -1.11,
		},
		{
			input: 0,
		},
		{
			input: 3.14,
		},
		{
			input: 167.42,
		},
	}

	var lastInt64 *int64
	for _, test := range tests {
		actual := Float64ToInt64(test.input)
		if lastInt64 != nil {
			// check that this float is greater than the last one
			if actual <= *lastInt64 {
				t.Errorf("expected greater than prev, this: %d, last %d", actual, *lastInt64)
			}
		}
		lastInt64 = &actual
		convertedBack := Int64ToFloat64(actual)
		// assert that we got back what we started with
		if convertedBack != test.input {
			t.Errorf("expected %f, got %f", test.input, convertedBack)
		}
	}
}
