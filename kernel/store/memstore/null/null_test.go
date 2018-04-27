package null

import (
	"testing"

	"github.com/tiglabs/baudengine/kernel/store/memstore"
)

func TestStore(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}

	NullTestMemStore(t, s)
}

// NullTestKVStore has very different expectations
// compared to CommonTestKVStore
func NullTestMemStore(t *testing.T, s memstore.MemStore) {

	batch := s.NewBatch()
	batch.Set([]byte("b"), []byte("val-b"))
	batch.Set([]byte("c"), []byte("val-c"))
	batch.Set([]byte("d"), []byte("val-d"))
	batch.Set([]byte("e"), []byte("val-e"))
	batch.Set([]byte("f"), []byte("val-f"))
	batch.Set([]byte("g"), []byte("val-g"))
	batch.Set([]byte("h"), []byte("val-h"))
	batch.Set([]byte("i"), []byte("val-i"))
	batch.Set([]byte("j"), []byte("val-j"))

	err := s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	err = s.RangeIterator([]byte("b"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
}
