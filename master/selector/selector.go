package selector

type Tag interface {
	Name() string
	Boost() float32
}

type Item interface {
	Tags() []Tag
}

type Iterator interface {
	Next() Item
}

type Selector interface {
	SelectTarget(iterator Iterator, count int) []Item
}
