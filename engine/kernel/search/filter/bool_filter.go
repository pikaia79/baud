package filter

type BoolFilter struct {
	must     []Filter
	must_not []Filter
	should   []Filter
}

func NewBoolFilter() *BoolFilter {
	return &BoolFilter{}
}

func (b *BoolFilter) AddMustFilter(f Filter) {
	b.must =  append(b.must, f)
}

func (b *BoolFilter) AddMustNotFilter(f Filter) {
	b.must_not = append(b.must_not, f)
}

func (b *BoolFilter) AddShouldFilter(f Filter) {
	b.should = append(b.should, f)
}
