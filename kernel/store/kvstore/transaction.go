package kvstore

type Transaction interface {
	Put(key, value []byte, ops ...*Option) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte, ops ...*Option) error

	// PrefixIterator returns a KVIterator that will
	// visit all K/V pairs with the provided prefix
	PrefixIterator(prefix []byte) KVIterator

	// RangeIterator returns a KVIterator that will
	// visit all K/V pairs >= start AND < end
	RangeIterator(start, end []byte) KVIterator

	Commit() error
	Rollback() error
}
