package test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/tiglabs/baud/kernel/store/memstore"
)

func CommonTestReaderOwnsGetValue(t *testing.T, s memstore.MemStore) {

	originalKey := []byte("key")
	originalVal := []byte("val")

	// write key/val
	batch := s.NewBatch()
	batch.Set(originalKey, originalVal)
	err := s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// read key
	rVal, err := s.Get(originalKey)
	if err != nil {
		t.Fatal(err)
	}
	returnedVal := rVal.([]byte)
	// check that it is the expected value
	if !reflect.DeepEqual(returnedVal, originalVal) {
		t.Fatalf("expected value: %v for '%s', got %v", originalVal, originalKey, returnedVal)
	}

	// mutate the returned value with reader still open
	for i := range []byte(returnedVal) {
		returnedVal[i] = '1'
	}

	// read the key again
	rVal2, err := s.Get(originalKey)
	if err != nil {
		t.Fatal(err)
	}

	returnedVal2 := rVal2.([]byte)
	// check that it is the expected value
	if !reflect.DeepEqual(returnedVal2, originalVal) {
		t.Fatalf("expected value: %v for '%s', got %v", originalVal, originalKey, returnedVal2)
	}

	// mutate the original returned value again
	for i := range returnedVal {
		returnedVal[i] = '2'
	}

	// read the key again
	rVal3, err := s.Get(originalKey)
	if err != nil {
		t.Fatal(err)
	}

	returnedVal3 := rVal3.([]byte)
	// check that it is the expected value
	if !reflect.DeepEqual(returnedVal3, originalVal) {
		t.Fatalf("expected value: %v for '%s', got %v", originalVal, originalKey, returnedVal3)
	}

	// finally check that the value we mutated still has what we set it to
	for i := range returnedVal {
		if returnedVal[i] != '2' {
			t.Errorf("expected byte to be '2', got %v", returnedVal[i])
		}
	}
}

func CommonTestWriterOwnsValues(t *testing.T, s memstore.MemStore) {

	keyBuffer := make([]byte, 5)
	valBuffer := make([]byte, 5)

	// write key/val pairs reusing same buffer
	batch := s.NewBatch()
	for i := 0; i < 10; i++ {
		keyBuffer[0] = 'k'
		keyBuffer[1] = 'e'
		keyBuffer[2] = 'y'
		keyBuffer[3] = '-'
		keyBuffer[4] = byte('0' + i)
		valBuffer[0] = 'v'
		valBuffer[1] = 'a'
		valBuffer[2] = 'l'
		valBuffer[3] = '-'
		valBuffer[4] = byte('0' + i)
		batch.Set(keyBuffer, valBuffer)
		valBuffer = make([]byte, 5)
	}
	err := s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// check that we can read back what we expect
	allks := make([][]byte, 0)
	allvs := make([][]byte, 0)
	err = s.RangeIterator(nil, nil, func(key []byte, value interface{}) bool {
		copyk := make([]byte, len(key))
		copy(copyk, key)
		allks = append(allks, copyk)
		v, ok := value.([]byte)
		if !ok {
			t.Fatal("invalid value")
		}
		copyv := make([]byte, len(v))
		copy(copyv, v)
		allvs = append(allvs, copyv)
		return true
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(allks) != 10 {
		t.Fatalf("expected 10 k/v pairs, got %d", len(allks))
	}
	for i, key := range allks {
		val := allvs[i]
		if !bytes.HasSuffix(key, []byte{byte('0' + i)}) {
			t.Errorf("expected key %v to end in %d", key, []byte{byte('0' + i)})
		}
		if !bytes.HasSuffix(val, []byte{byte('0' + i)}) {
			t.Errorf("expected val %v to end in %d", string(val), []byte{byte('0' + i)})
		}
	}

	// now delete using same approach
	batch = s.NewBatch()
	for i := 0; i < 10; i++ {
		keyBuffer[0] = 'k'
		keyBuffer[1] = 'e'
		keyBuffer[2] = 'y'
		keyBuffer[3] = '-'
		keyBuffer[4] = byte('0' + i)
		batch.Delete(keyBuffer)
	}
	err = s.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// check that we can read back what we expect
	allks = make([][]byte, 0)
	err = s.RangeIterator(nil, nil, func(key []byte, value interface{}) bool {
		copyk := make([]byte, len(key))
		copy(copyk, key)
		allks = append(allks, copyk)
		v := value.([]byte)
		copyv := make([]byte, len(v))
		copy(copyv, v)
		allvs = append(allvs, copyv)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(allks) != 0 {
		t.Fatalf("expected 0 k/v pairs remaining, got %d", len(allks))
	}
}
