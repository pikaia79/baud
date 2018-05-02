package kvstore

type KVStore interface {
	Put(key, value []byte, ops ...*Option) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte, ops ...*Option) error

	// MultiGet retrieves multiple values in one call.
	MultiGet(keys [][]byte) ([][]byte, error)

	NewTransaction(writable bool) (Transaction, error)

	// PrefixIterator returns a KVIterator that will
	// visit all K/V pairs with the provided prefix
	PrefixIterator(prefix []byte) KVIterator

	// RangeIterator returns a KVIterator that will
	// visit all K/V pairs >= start AND < end
	RangeIterator(start, end []byte) KVIterator

	NewKVBatch() KVBatch
	ExecuteBatch(batch KVBatch, ops ...*Option) error

	// GetSnapshot returns a Snapshot which can be used to
	// read data from the KVStore.  If a Snapshot cannot
	// be obtained a non-nil error is returned.
	GetSnapshot() (Snapshot, error)
	Close() error
}

// Snapshot is an abstraction of an **ISOLATED** reader
// In this context isolated is defined to mean that
// writes/deletes made after the Snapshot is opened
// are not observed.
// Because there is usually a cost associated with
// keeping isolated readers active, users should
// close them as soon as they are no longer needed.
type Snapshot interface {

	// Get returns the value associated with the key
	// If the key does not exist, nil is returned.
	// The caller owns the bytes returned.
	Get(key []byte) ([]byte, error)

	// Get last option which is Set by store.
	// We can use it to get last raft apply ID of the snapshot.
	LastOption() (*Option, error)

	// MultiGet retrieves multiple values in one call.
	MultiGet(keys [][]byte) ([][]byte, error)

	// PrefixIterator returns a KVIterator that will
	// visit all K/V pairs with the provided prefix
	PrefixIterator(prefix []byte) KVIterator

	// RangeIterator returns a KVIterator that will
	// visit all K/V pairs >= start AND < end
	RangeIterator(start, end []byte) KVIterator

	// Close closes the iterator
	Close() error
}

// KVIterator is an abstraction around key iteration
type KVIterator interface {

	// Seek will advance the iterator to the specified key
	Seek(key []byte)

	// Next will advance the iterator to the next key
	Next()

	// Key returns the key pointed to by the iterator
	// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
	// Continued use after that requires that they be copied.
	Key() []byte

	// Value returns the value pointed to by the iterator
	// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
	// Continued use after that requires that they be copied.
	Value() []byte

	// Valid returns whether or not the iterator is in a valid state
	Valid() bool

	// Current returns Key(),Value(),Valid() in a single operation
	Current() ([]byte, []byte, bool)

	// Close closes the iterator
	Close() error
}

type KVBatch interface {

	// Set updates the key with the specified value
	// both key and value []byte may be reused as soon as this call returns
	Set(key, val []byte)

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

// MultiGet is a helper function to retrieve mutiple keys from a
// KVReader, and might be used by KVStore implementations that don't
// have a native multi-get facility.
func MultiGet(reader Snapshot, keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, len(keys))

	for i, key := range keys {
		val, err := reader.Get(key)
		if err != nil {
			return nil, err
		}

		vals[i] = val
	}

	return vals, nil
}

type Option struct {
	// only for raft when write
	ApplyID uint64
}
