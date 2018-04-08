package llrbdb

import (
	"github.com/tiglabs/baud/kernel/store/memstore"
)

var _ memstore.MemStore = &Store{}

type Store struct {
	db     *DB
}

func New() (memstore.MemStore, error) {
	db, err := NewDB()
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Put(key []byte, value interface{}) error {
	if s == nil {
		return nil
	}
	return s.db.Update(func(tx *Tx) error {
		_,_,err := tx.Set(key, value)
		if err != nil {
			return err
		}
		return nil
	})
}
func (s *Store) Get(key []byte) (val interface{}, err error) {
	if s == nil {
		return nil, nil
	}
	err = s.db.View(func(tx *Tx) error {
		val, err = tx.Get(key)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *Store) Delete(key []byte) error {
	if s == nil {
		return nil
	}
	return s.db.Update(func(tx *Tx) error {
		_,err := tx.Delete(key)
		if err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) PrefixIterator(prefix []byte, iter memstore.IterFunc) error {
	if s == nil || iter == nil {
		return nil
	}
	tx, err := s.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var _prefix []byte
	_prefix = make([]byte, len(prefix) + 1)
	copy(_prefix, prefix)
	_prefix[len(prefix)] = byte('*')
	return tx.AscendKeys(_prefix, func(key []byte, value interface{}) bool {
		return iter(key, value)
	})
}

func (s *Store) RangeIterator(start, end []byte, iter memstore.IterFunc) error {
	if s == nil || iter == nil {
		return nil
	}
	tx, err := s.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return tx.AscendRange(start, end, func(key []byte, value interface{}) bool {
		return iter(key, value)
	})
}

func (s *Store) NewBatch() memstore.MemBatch {
	return memstore.NewBatch()
}

func (s *Store) ExecuteBatch(batch memstore.MemBatch) (err error) {
	if s == nil {
		return nil
	}
	if batch == nil {
		return nil
	}
	var tx *Tx
	tx, err= s.db.Begin(true)
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

	for _, op := range batch.Operations() {
		if op.Value() != nil {
			_,_,err = tx.Set(op.Key(), op.Value())
			if err != nil {
				return
			}
		} else {
			_,err = tx.Delete(op.Key())
			if err != nil {
				return
			}
		}
	}
	return
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	return s.db.Close()
}


