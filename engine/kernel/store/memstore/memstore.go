package memstore

type IterFunc func(key []byte, value interface{}) bool

type MemStore interface {
	// value is an object for memory, and it can be changed out of the store
	Put(key []byte, value interface{}) error
	Get(key []byte) (interface{}, error)
	Delete(key []byte) error

	// PrefixIterator will visit all K/V pairs with the provided prefix.
	// Returns an error if the store is closed or the tx is closed.
	// Because it will block the write operation, users should
	// not do slow operation as much as possible.
	PrefixIterator(prefix []byte, iter IterFunc) error

	// RangeIterator will visit all K/V pairs >= start AND < end
	// Returns an error if the store is closed or the tx is closed.
	// Because it will block the write operation, users should
	// not do slow operation as much as possible.
	RangeIterator(start, end []byte, iter IterFunc) error

	NewBatch() MemBatch
	ExecuteBatch(batch MemBatch) error

	Close() error
}

type MemBatch interface {

	// Set updates the key with the specified value
	// both key and value []byte may be reused as soon as this call returns
	Set(key []byte, val interface{})

	// Delete removes the specified key
	// the key []byte may be reused as soon as this call returns
	Delete(key []byte)

	// Return all operation
	Operations() []Operation

	// Reset frees resources for this batch and allows reuse
	Reset()

	// Close frees resources
	Close() error
}
