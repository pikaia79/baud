package analysis

import "time"

type Analyzer interface {
	Analyze(val []byte) []*Token
}

type DateTimeParser interface {
	ParseDateTime(string) (time.Time, error)
}
