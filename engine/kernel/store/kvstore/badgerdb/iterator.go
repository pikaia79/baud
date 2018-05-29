package badgerdb

import (
	"bytes"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
)

var _ kvstore.KVIterator = &Iterator{}

type Iterator struct {
	close  sync.Once
	tx     *badger.Txn
	iter   *badger.Iterator
	prefix []byte
	start  []byte
	end    []byte
	valid  bool
	key    []byte
	val    []byte
}

func (i *Iterator) updateValid() {
	i.valid = (i.key != nil)
	if i.valid {
		if i.prefix != nil {
			i.valid = bytes.HasPrefix(i.key, i.prefix)
		} else if i.end != nil {
			i.valid = bytes.Compare(i.key, i.end) < 0
		}
	}
}

func (i *Iterator) Seek(k []byte) {
	if i == nil {
		return
	}
	if i.start != nil && bytes.Compare(k, i.start) < 0 {
		k = i.start
	}
	if i.prefix != nil && !bytes.HasPrefix(k, i.prefix) {
		if bytes.Compare(k, i.prefix) < 0 {
			k = i.prefix
		} else {
			i.valid = false
			return
		}
	}
	var err error
	i.iter.Seek(k)
	if i.iter.Valid() {
		item := i.iter.Item()
		i.key = item.Key()
		i.val, err = item.Value()
		if err != nil {
			i.key = nil
			i.val = nil
		}
	} else {
		i.key = nil
		i.val = nil
	}
	i.updateValid()
}

func (i *Iterator) Next() {
	if i == nil {
		return
	}
	var err error
	i.iter.Next()
	if i.iter.Valid() {
		item := i.iter.Item()
		i.key = item.Key()
		i.val, err = item.Value()
		if err != nil {
			i.key = nil
			i.val = nil
		}
	} else {
		i.key = nil
		i.val = nil
	}
	i.updateValid()
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i == nil {
		return nil, nil, false
	}
	return i.key, i.val, i.valid
}

func (i *Iterator) Key() []byte {
	if i == nil {
		return nil
	}
	return i.key
}

func (i *Iterator) Value() []byte {
	if i == nil {
		return nil
	}
	return i.val
}

func (i *Iterator) Valid() bool {
	if i == nil {
		return false
	}
	return i.valid
}

func (i *Iterator) Close() error {
	if i == nil {
		return nil
	}
	i.close.Do(func() {
		if i.iter != nil {
			i.iter.Close()
		}
		if i.tx != nil {
			i.tx.Discard()
		}
	})

	return nil
}
