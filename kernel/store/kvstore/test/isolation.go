package test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/tiglabs/baud/kernel/store/kvstore"
)

func CommonTestReaderIsolation(t *testing.T, s kvstore.KVStore) {
	hackSize := 1000
	batch := s.NewKVBatch()
	for i := 0; i < hackSize; i++ {
		k := fmt.Sprintf("x%d", i)
		batch.Set([]byte(k), []byte("filler"))
	}
	err := s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	// **************************************************

	batch = s.NewKVBatch()
	batch.Set([]byte("a"), []byte("val-a"))
	err = s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// create an isolated reader
	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// verify that we see the value already inserted
	val, err := reader.Get([]byte("a"))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(val, []byte("val-a")) {
		t.Errorf("expected val-a, got nil")
	}

	// verify that an iterator sees it
	count := 0
	it := reader.RangeIterator([]byte{0}, []byte{'x'})
	defer func() {
		err := it.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	for it.Valid() {
		it.Next()
		count++
	}
	if count != 1 {
		t.Errorf("expected iterator to see 1, saw %d", count)
	}

	batch = s.NewKVBatch()
	batch.Set([]byte("b"), []byte("val-b"))
	err = s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// ensure that a newer reader sees it
	newReader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := newReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	val, err = newReader.Get([]byte("b"))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(val, []byte("val-b")) {
		t.Errorf("expected val-b, got nil")
	}

	// ensure that the direct iterator sees it
	count = 0
	it2 := newReader.RangeIterator([]byte{0}, []byte{'x'})
	defer func() {
		err := it2.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	for it2.Valid() {
		it2.Next()
		count++
	}
	if count != 2 {
		t.Errorf("expected iterator to see 2, saw %d", count)
	}

	// but that the isolated reader does not
	val, err = reader.Get([]byte("b"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}

	// and ensure that the iterator on the isolated reader also does not
	count = 0
	it3 := reader.RangeIterator([]byte{0}, []byte{'x'})
	defer func() {
		err := it3.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	for it3.Valid() {
		it3.Next()
		count++
	}
	if count != 1 {
		t.Errorf("expected iterator to see 1, saw %d", count)
	}

}
