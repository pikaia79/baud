package analysis

type Analyzer interface {
	Analyze(val []byte) []*Token
}
