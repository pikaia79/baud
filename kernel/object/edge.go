package object

type Edge struct {
	src UID
	dst UID
	doc []byte
	version uint32
}

func NewEdge(src, dst UID, doc []byte) *Edge {
	return &Edge{src: src, dst: dst, doc: doc}
}

