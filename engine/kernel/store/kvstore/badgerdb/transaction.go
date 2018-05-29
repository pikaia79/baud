package badgerdb

import (
	"github.com/dgraph-io/badger"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
)

type Transaction struct {
	writable bool
	tx *badger.Txn
}

func (bs *Store) NewTransaction(writable bool) (kvstore.Transaction, error) {
	tx := bs.db.NewTransaction(writable)
	return &Transaction{tx: tx, writable: writable}, nil
}

func(tx *Transaction) Put(key, value []byte) error {
	err := tx.tx.Set(key, value)
	if err != nil {
		return err
	}
	return nil
}

func(tx *Transaction) Get(key []byte) ([]byte, error) {
	v, err := tx.tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var value []byte
	return v.ValueCopy(value)
}

func(tx *Transaction) Delete(key []byte) error {
	err := tx.tx.Delete(key)
	if err != nil {
		return err
	}
	return nil
}

func(tx *Transaction) PrefixIterator(prefix []byte) kvstore.KVIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := tx.tx.NewIterator(opts)
	rv := &Iterator{
		iter:   it,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

func(tx *Transaction) RangeIterator(start, end []byte) kvstore.KVIterator {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := tx.tx.NewIterator(opts)
	rv := &Iterator{
		iter:   it,
		start:  start,
		end:    end,
	}

	rv.Seek(start)
	return rv
}

func(tx *Transaction) Commit() error {
	if tx == nil {
		return nil
	}
	if tx.writable {
		return tx.tx.Commit(nil)
	} else {
		tx.tx.Discard()
	}
	return nil
}

func(tx *Transaction) Rollback() error {
	if tx == nil {
		return nil
	}
	tx.tx.Discard()
	return nil
}
