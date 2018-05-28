package query

type MatchQuery struct {
	fieldId    uint32
	query      []byte
	// or/and, default or
	operator   string

	minimum_should_match float32

	// default 1
	boost int

	search_analyzer string
}

func NewMatchQuery(fieldId uint32, query []byte) *MatchQuery {
	return &MatchQuery{fieldId: fieldId, query: query, operator: "or", minimum_should_match: 1.0, boost: 1}
}

func (m *MatchQuery) SetOperator(op string) {
	if op != "or" && op != "and" {
		panic("invalid operator")
	}
	m.operator = op
}

func (m *MatchQuery) SetMinimumShouldMatch(min float32) {
	m.minimum_should_match = min
}

func (m *MatchQuery) SetBoost(boost int) {
	m.boost = boost
}

func (m *MatchQuery) SetSearchAnalyzer(analyzer string) {
	m.search_analyzer = analyzer
}