package query

import (
	"testing"
)

type MinShouldGroup struct {
	input      string
	output     float64
	maxShould  int
}

func TestParseMinShould(t *testing.T) {
	test := []MinShouldGroup{
		MinShouldGroup{
			input:  "3",
			output: 3.0,
			maxShould: 10,
		},
		MinShouldGroup{
			input: "-1",
			output: 9.0,
			maxShould: 10,
		},
		MinShouldGroup{
			input: "75%",
			output: 7,
			maxShould: 10,
		},
		MinShouldGroup{
			input: "-25%",
			output: 8,
			maxShould: 10,
		},
		MinShouldGroup{
			input: "3<-25%",
			output: 8,
			maxShould: 10,
		},
		MinShouldGroup{
			input: "3<25%",
			output: 2,
			maxShould: 10,
		},
	}
	for _, tt := range test {
		min, err := ParseMinShould([]byte(tt.input), tt.maxShould)
		if err != nil {
			t.Fatalf("%v, parse err %v", tt, err)
		}
		if min != tt.output {
			t.Fatalf("%v parse failed, %d, %d", tt, tt.output, min)
		}
	}
}
