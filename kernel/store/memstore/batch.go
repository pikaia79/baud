package memstore

var _ Operation = &op{}
var _ MemBatch = &Batch{}

type Operation interface {
	Key() []byte
	Value() interface{}
}

type op struct {
	K []byte
	V interface{}
}

func (o *op) Key() []byte {
	if o == nil {
		return nil
	}
	return o.K
}

func (o *op) Value() interface{} {
	if o == nil {
		return nil
	}
	return o.V
}

type Batch struct {
	Ops []Operation
}

func NewBatch() *Batch {
	return &Batch{
		Ops: make([]Operation, 0, 1000),
	}
}

func (b *Batch) Set(key []byte, val interface{}) {
	ck := make([]byte, len(key))
	copy(ck, key)
	b.Ops = append(b.Ops, &op{ck, val})
}

func (b *Batch) Delete(key []byte) {
	ck := make([]byte, len(key))
	copy(ck, key)
	b.Ops = append(b.Ops, &op{ck, nil})
}

func (b *Batch) Operations() []Operation {
	return b.Ops
}

// for reuse
func (b *Batch) Reset() {
	b.Ops = b.Ops[:0]
}

func (b *Batch) Close() error {
	return nil
}
