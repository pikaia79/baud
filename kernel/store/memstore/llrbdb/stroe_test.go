package llrbdb

import (
	"testing"

	"github.com/tiglabs/baud/kernel/store/memstore"
	"github.com/tiglabs/baud/kernel/store/memstore/test"
)

func open(t *testing.T) memstore.MemStore {
	rv, err := New()
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func cleanup(t *testing.T, s memstore.MemStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoltDBKVCrud(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestBoltDBReaderOwnsGetBytes(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetValue(t, s)
}

func TestBoltDBWriterOwnsBytes(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsValues(t, s)
}

func TestBoltDBPrefixIterator(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestBoltDBRangeIterator(t *testing.T) {
	s := open(t)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}
