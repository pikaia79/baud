package test

import (
	"testing"

	"github.com/tiglabs/baudengine/kernel/store/memstore"
)

// basic crud tests

func CommonTestKVCrud(t *testing.T, s memstore.MemStore) {
	batch := s.NewBatch()
	batch.Set([]byte("a"), []byte("val-a"))
	batch.Set([]byte("z"), []byte("val-z"))
	batch.Delete([]byte("z"))
	err := s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	batch.Reset()

	batch.Set([]byte("b"), []byte("val-b"))
	batch.Set([]byte("c"), []byte("val-c"))
	batch.Set([]byte("d"), []byte("val-d"))
	batch.Set([]byte("e"), []byte("val-e"))
	batch.Set([]byte("f"), []byte("val-f"))
	batch.Set([]byte("g"), []byte("val-g"))
	batch.Set([]byte("h"), []byte("val-h"))
	batch.Set([]byte("i"), []byte("val-i"))
	batch.Set([]byte("j"), []byte("val-j"))

	err = s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	err = s.RangeIterator([]byte("b"), []byte("h"), func(key []byte, value interface{}) bool {
		count++
		if count == 1 {
			if string(key) != "b" {
				t.Fatalf("expected key b, got %s", key)
			}
			if string(value.([]byte)) != "val-b" {
				t.Fatalf("expected value val-b, got %s", value)
			}
		} else if count == 2 {
			if string(key) != "c" {
				t.Fatalf("expected key c, got %s", key)
			}
			if string(value.([]byte)) != "val-c" {
				t.Fatalf("expected value val-c, got %s", value)
			}
		} else if count >= 7 {
			t.Fatalf(`expected ["b","h"), got %s`, key)
			return false
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}
