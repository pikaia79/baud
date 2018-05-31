package query

type Boost float64

func (b *Boost) Value() float64 {
	if b == nil {
		return 1.0
	}
	return float64(*b)
}

func NewBoost(f float64) *Boost {
	b := Boost(f)
	return &b
}