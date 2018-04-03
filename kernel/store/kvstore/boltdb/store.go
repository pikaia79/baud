package boltdb

import (
	"os"
	"errors"

	"github.com/boltdb/bolt"
	"github.com/tiglabs/baud/kernel/store/kvstore"
)

type StoreConfig struct {
	Path       string
	Bucket     string
	NoSync     bool
	ReadOnly   bool
	FillPercent float64
}

type Store struct {
	path        string
	bucket      []byte
	db          *bolt.DB
	noSync      bool
	fillPercent float64
}

func New(config *StoreConfig) (kvstore.KVStore, error) {
	if config == nil {
		return nil, errors.New("must provide config")
	}
	if config.Path == "" {
		return nil, os.ErrInvalid
	}
	path := config.Path
	bucket := config.Bucket
	if config.Bucket == "" {
		bucket = "baud"
	}
	noSync := config.NoSync
	fillPercent := config.FillPercent
	if fillPercent == 0.0 {
		fillPercent = bolt.DefaultFillPercent
	}

	bo := &bolt.Options{}
	bo.ReadOnly = config.ReadOnly

	db, err := bolt.Open(path, 0600, bo)
	if err != nil {
		return nil, err
	}
	db.NoSync = noSync

	if !bo.ReadOnly {
		err = db.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte(bucket))

			return err
		})
		if err != nil {
			return nil, err
		}
	}

	rv := Store{
		path:        path,
		bucket:      []byte(bucket),
		db:          db,
		noSync:      noSync,
		fillPercent: fillPercent,
	}
	return &rv, nil
}

func (bs *Store)Get(key []byte) (value []byte, err error) {
	if bs == nil {
		return nil, nil
	}
	err = bs.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bs.bucket)
		v := b.Get(key)
		if v != nil {
			value = cloneBytes(v)
		}
		return nil
	})
	return
}

func (bs *Store)Put(key []byte, value []byte) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bs.bucket)
		err := b.Put(key, value)
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *Store)Delete(key []byte) error {
	if bs == nil {
		return nil
	}
	return bs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bs.bucket)
		err := b.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
}

func (bs *Store)MultiGet(keys [][]byte) ([][]byte, error) {
	if bs == nil {
		return nil, nil
	}
	r, err := bs.Reader()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return r.MultiGet(keys)
}

func (bs *Store) Reader() (kvstore.KVReader, error) {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &Reader{
		tx:     tx,
		bucket: tx.Bucket(bs.bucket),
	}, nil
}

func (bs *Store)PrefixIterator(prefix []byte) kvstore.KVIterator {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return nil
	}
	cursor := tx.Bucket([]byte(bs.bucket)).Cursor()

	rv := &Iterator{
		tx:     tx,
		cursor: cursor,
		prefix: prefix,
	}

	rv.Seek(prefix)
	return rv
}

func (bs *Store)RangeIterator(start, end []byte) kvstore.KVIterator {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return nil
	}
	cursor := tx.Bucket([]byte(bs.bucket)).Cursor()
	rv := &Iterator{
		tx:     tx,
		cursor: cursor,
		start:  start,
		end:    end,
	}

	rv.Seek(start)
	return rv
}

func (bs *Store)NewKVBatch() kvstore.KVBatch {
	return kvstore.NewBatch()
}

func (bs *Store)ExecuteBatch(batch kvstore.KVBatch) (err error) {
	if bs == nil {
		return nil
	}
	if batch == nil {
		return nil
	}
	var tx *bolt.Tx
	tx, err = bs.db.Begin(true)
	if err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	bucket := tx.Bucket([]byte(bs.bucket))
	bucket.FillPercent = bs.fillPercent

	for _, op := range batch.Operations() {
		if op.Value() != nil {
			err = bucket.Put(op.Key(), op.Value())
			if err != nil {
				return
			}
		} else {
			err = bucket.Delete(op.Key())
			if err != nil {
				return
			}
		}
	}
	return
}

func (bs *Store) Close() error {
	return bs.db.Close()
}

func cloneBytes(b []byte) []byte {
	return append([]byte(nil), b...)
}
