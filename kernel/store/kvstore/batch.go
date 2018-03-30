package kvstore

var _ Operation = &op{}
var _ KVBatch = &Batch{}

type Operation interface {
	Key() []byte
	Value() []byte
}

type op struct {
	K []byte
	V []byte
}

func (o *op)Key() []byte {
	if o == nil {
		return nil
	}
	return o.K
}

func (o *op)Value() []byte {
	if o == nil {
		return nil
	}
	return o.V
}

type Batch struct {
	Ops    []Operation
}

func NewBatch() *Batch {
	return &Batch{
		Ops:    make([]Operation, 0, 1000),
	}
}

func (b *Batch) Set(key, val []byte) {
	ck := make([]byte, len(key))
	copy(ck, key)
	cv := make([]byte, len(val))
	copy(cv, val)
	b.Ops = append(b.Ops, &op{ck, cv})
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
