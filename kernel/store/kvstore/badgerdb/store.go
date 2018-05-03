package badgerdb

import (
	"errors"
	"os"

	"github.com/dgraph-io/badger"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
)

var _ kvstore.KVStore = &Store{}

type StoreConfig struct {
	Path     string
	Sync     bool
	ReadOnly bool
}

type Store struct {
	path string
	db   *badger.DB
}

func New(config *StoreConfig) (kvstore.KVStore, error) {
	if config == nil {
		return nil, errors.New("must provide config")
	}
	if config.Path == "" {
		return nil, os.ErrInvalid
	}
	path := config.Path
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	if !config.Sync {
		opts.SyncWrites = false
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	rv := Store{
		path: path,
		db:   db,
	}
	return &rv, nil
}

func (bs *Store) Get(key []byte) (value []byte, err error) {
	if bs == nil {
		return nil, nil
	}
	err = bs.db.View(func(tx *badger.Txn) error {
		v, err := tx.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		value, err = v.ValueCopy(value)
		return err
	})
	return
}

func (bs *Store) Put(key []byte, value []byte) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *badger.Txn) error {
		err := tx.Set(key, value)
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *Store) Delete(key []byte) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *badger.Txn) error {
		err := tx.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *Store) MultiGet(keys [][]byte) ([][]byte, error) {
	if bs == nil {
		return nil, nil
	}
	snap, err := bs.GetSnapshot()
	if err != nil {
		return nil, err
	}
	defer snap.Close()
	return snap.MultiGet(keys)
}

func (bs *Store) GetSnapshot() (kvstore.Snapshot, error) {
	tx := bs.db.NewTransaction(false)
	return &Snapshot{
		tx: tx,
	}, nil
}

func (bs *Store) PrefixIterator(prefix []byte) kvstore.KVIterator {
	tx := bs.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := tx.NewIterator(opts)
	rv := &Iterator{
		tx:     tx,
		iter:   it,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

func (bs *Store) RangeIterator(start, end []byte) kvstore.KVIterator {
	tx := bs.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := tx.NewIterator(opts)
	rv := &Iterator{
		tx:    tx,
		iter:  it,
		start: start,
		end:   end,
	}

	rv.Seek(start)
	return rv
}

func (bs *Store) NewKVBatch() kvstore.KVBatch {
	return kvstore.NewBatch()
}

func (bs *Store) ExecuteBatch(batch kvstore.KVBatch) (err error) {
	if bs == nil {
		return nil
	}
	if batch == nil {
		return nil
	}
	var tx *badger.Txn
	tx = bs.db.NewTransaction(true)
	defer func() {
		if err != nil {
			tx.Discard()
		}
	}()

	for _, op := range batch.Operations() {
		if op.Value() != nil {
			err = tx.Set(op.Key(), op.Value())
		} else {
			err = tx.Delete(op.Key())
		}
		if err == badger.ErrTxnTooBig {
			err = tx.Commit(nil)
			if err != nil {
				return
			}
			tx = bs.db.NewTransaction(true)
		}
	}
	err = tx.Commit(nil)
	return
}

func (bs *Store) Close() error {
	if bs == nil {
		return nil
	}
	return bs.db.Close()
}

func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
