package test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/tiglabs/baud/kernel/store/memstore"
)

// tests around the correct behavior of iterators

type testRow struct {
	key []byte
	val interface{}
}

func batchWriteRows(s memstore.MemStore, rows []testRow) error {
	batch := s.NewBatch()
	for _, row := range rows {
		batch.Set(row.key, row.val)
	}
	err := s.ExecuteBatch(batch)
	if err != nil {
		return err
	}

	return nil
}

func CommonTestPrefixIterator(t *testing.T, s memstore.MemStore) {

	data := []testRow{
		{[]byte("apple"), []byte("val")},
		{[]byte("cat1"), []byte("val")},
		{[]byte("cat2"), []byte("val")},
		{[]byte("cat3"), []byte("val")},
		{[]byte("dog1"), []byte("val")},
		{[]byte("dog2"), []byte("val")},
		{[]byte("dog4"), []byte("val")},
		{[]byte("elephant"), []byte("val")},
	}

	expectedCats := [][]byte{
		[]byte("cat1"),
		[]byte("cat2"),
		[]byte("cat3"),
	}

	expectedDogs := [][]byte{
		[]byte("dog1"),
		[]byte("dog2"),
		[]byte("dog4"),
	}

	err := batchWriteRows(s, data)
	if err != nil {
		t.Fatal(err)
	}

	// get a prefix reader
	cats := make([][]byte, 0)
	err = s.PrefixIterator([]byte("cat"), func(key []byte, val interface{})bool {
		k := key
		copyk := make([]byte, len(k))
		copy(copyk, k)
		cats = append(cats, copyk)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that we found all the cats
	if !reflect.DeepEqual(cats, expectedCats) {
		t.Fatalf("expected cats %v, got %v", expectedCats, cats)
	}

	// get a prefix reader
	dogs := make([][]byte, 0)
	err = s.PrefixIterator([]byte("dog"), func(key []byte, val interface{})bool {
		copyk := make([]byte, len(key))
		copy(copyk, key)
		dogs = append(dogs, copyk)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that we found the expected dogs
	if !reflect.DeepEqual(dogs, expectedDogs) {
		t.Fatalf("expected dogs %v, got %v", expectedDogs, dogs)
	}
}

func CommonTestRangeIterator(t *testing.T, s memstore.MemStore) {

	data := []testRow{
		{[]byte("a1"), []byte("val")},
		{[]byte("b1"), []byte("val")},
		{[]byte("b2"), []byte("val")},
		{[]byte("b3"), []byte("val")},
		{[]byte("c1"), []byte("val")},
		{[]byte("c2"), []byte("val")},
		{[]byte("c4"), []byte("val")},
		{[]byte("d1"), []byte("val")},
	}

	expectedAll := make([][]byte, 0)
	expectedBToC := make([][]byte, 0)
	expectedCToDSeek3 := make([][]byte, 0)
	expectedCToEnd := make([][]byte, 0)
	for _, row := range data {
		expectedAll = append(expectedAll, row.key)
		if bytes.HasPrefix(row.key, []byte("b")) {
			expectedBToC = append(expectedBToC, row.key)
		}
		if bytes.HasPrefix(row.key, []byte("c")) && !bytes.HasSuffix(row.key, []byte("2")) {
			expectedCToDSeek3 = append(expectedCToDSeek3, row.key)
		}
		if bytes.Compare(row.key, []byte("c")) > 0 {
			expectedCToEnd = append(expectedCToEnd, row.key)
		}
	}

	err := batchWriteRows(s, data)
	if err != nil {
		t.Fatal(err)
	}

	// get a range iterator (all)
	all := make([][]byte, 0)
	err = s.RangeIterator(nil, nil, func(key []byte, val interface{})bool {
		copyk := make([]byte, len(key))
		copy(copyk, key)
		all = append(all, copyk)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that we found all
	if !reflect.DeepEqual(all, expectedAll) {
		t.Fatalf("expected all %v, got %v", expectedAll, all)
	}

	// get range iterator from b - c
	bToC := make([][]byte, 0)
	err = s.RangeIterator([]byte("b"), []byte("c"), func(key []byte, val interface{})bool {
		copyk := make([]byte, len(key))
		copy(copyk, key)
		bToC = append(bToC, copyk)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that we found b-c
	if !reflect.DeepEqual(bToC, expectedBToC) {
		t.Fatalf("expected b-c %v, got %v", expectedBToC, bToC)
	}

	// get range iterator from c - d, but seek to 'c3'
	cToDSeek3 := make([][]byte, 0)
	err = s.RangeIterator([]byte("c"), []byte("d"), func(key []byte, val interface{})bool{
		// skip c2
		if bytes.Compare(key, []byte("c2")) == 0 {
			return true
		}
		copyk := make([]byte, len(key))
		copy(copyk, key)
		cToDSeek3 = append(cToDSeek3, copyk)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that we found c-d with seek to c3
	if !reflect.DeepEqual(cToDSeek3, expectedCToDSeek3) {
		t.Fatalf("expected b-c %v, got %v", expectedCToDSeek3, cToDSeek3)
	}

	// get range iterator from c to the end
	cToEnd := make([][]byte, 0)
	err = s.RangeIterator([]byte("c"), nil, func(key []byte, val interface{})bool{
		copyk := make([]byte, len(key))
		copy(copyk, key)
		cToEnd = append(cToEnd, copyk)
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	// check that we found c to end
	if !reflect.DeepEqual(cToEnd, expectedCToEnd) {
		t.Fatalf("expected b-c %v, got %v", expectedCToEnd, cToEnd)
	}
}

