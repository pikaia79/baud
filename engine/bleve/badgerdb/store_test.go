package badgerdb

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store/test"
	"github.com/blevesearch/bleve/index/store"
)

func open(t *testing.T) store.KVStore {
	config := make(map[string]interface{})
	config["path"] = "test"
	rv, err := New(nil, config)
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = os.RemoveAll("test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestBadgerDBKVCrud(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestBadgerDBReaderIsolation(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestBadgerDBReaderOwnsGetBytes(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestBadgerDBWriterOwnsBytes(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestBadgerDBPrefixIterator(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestBadgerDBPrefixIteratorSeek(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestBadgerDBRangeIterator(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestBadgerDBRangeIteratorSeek(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

