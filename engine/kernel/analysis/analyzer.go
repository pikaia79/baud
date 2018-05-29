package analysis

import "time"

type Analyzer interface {
	Analyze([]byte) TokenSet
}

type DateTimeParser interface {
	ParseDateTime(string) (time.Time, error)
}
