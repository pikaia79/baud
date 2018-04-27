package triedb

import (
	"sync"
	"errors"
	"bytes"

	"github.com/tiglabs/baudengine/util/match"
)

type IterFunc func(key []byte, value interface{}) bool

type dbItem struct {
	key     []byte
	value   interface{}
}

func (dbi *dbItem) Key() []byte {
	if dbi == nil {
		return nil
	}
	return dbi.key
}

func (dbi *dbItem) Value() interface{} {
	if dbi == nil {
		return nil
	}
	return dbi.value
}

type Tx struct {
	db       *DB             // the underlying database.
	writable bool            // when false mutable operations fail.
	funcd    bool            // when true Commit and Rollback panic.
	wc       *txWriteContext // context for writable transactions.
}

type txWriteContext struct {
	rbKeys *Trie      // a tree of all item ordered by key

	rollbackItems   map[string]*dbItem // details for rolling back tx.
	iterCount       int                // stack of iterators
}

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	ErrInvalidConfig = errors.New("invalid db config")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

type DB struct {
	mu        sync.RWMutex
	keys      *Trie

	closed    bool              // set when the database has been closed
}

func NewDB() (*DB, error) {
	return &DB{keys: NewTrie()}, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true

	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	db.keys = nil
	return nil
}


// Begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		// writable transactions have a writeContext object that
		// contains information about changes to the database.
		tx.wc = &txWriteContext{}
		tx.wc.rollbackItems = make(map[string]*dbItem)
	}
	return tx, nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// rollbackInner handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollbackInner() {
	// rollback the deleteAll if needed
	if tx.wc.rbKeys != nil {
		tx.db.keys = tx.wc.rbKeys
	}
	for key, item := range tx.wc.rollbackItems {
		tx.db.deleteFromDatabase(&dbItem{key: []byte(key)})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			tx.db.insertIntoDatabase(item)
		}
	}
}

// Commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (tx *Tx) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// Rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
func (tx *Tx) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	if tx.writable {
		tx.rollbackInner()
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

func (tx *Tx) Set(key []byte, value interface{}) (preValue interface{},
replaced bool, err error) {
	if tx.db == nil {
		return nil, false, ErrTxClosed
	} else if !tx.writable {
		return nil, false, ErrTxNotWritable
	} else if tx.wc.iterCount > 0 {
		return nil, false, ErrTxIterating
	}
	item := &dbItem{key: key, value: value}
	// Insert the item into the keys tree.
	prev := tx.db.insertIntoDatabase(item)

	// insert into the rollback map if there has not been a deleteAll.
	if tx.wc.rbKeys == nil {
		if prev == nil {
			// An item with the same key did not previously exist. Let's
			// create a rollback entry with a nil value. A nil value indicates
			// that the entry should be deleted on rollback. When the value is
			// *not* nil, that means the entry should be reverted.
			tx.wc.rollbackItems[string(key)] = nil
		} else {
			// A previous item already exists in the database. Let's create a
			// rollback entry with the item as the value. We need to check the
			// map to see if there isn't already an item that matches the
			// same key.
			if _, ok := tx.wc.rollbackItems[string(key)]; !ok {
				tx.wc.rollbackItems[string(key)] = prev
			}
			preValue, replaced = prev.value, true
		}
	}
	return preValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist then ErrNotFound is returned.
func (tx *Tx) Get(key []byte) (val interface{}, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}

	val = tx.db.get(key)
	if val == nil {
		// The item does not exists
		return nil, ErrNotFound
	}
	return
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Delete(key []byte) (val interface{}, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if !tx.writable {
		return nil, ErrTxNotWritable
	} else if tx.wc.iterCount > 0 {
		return nil, ErrTxIterating
	}
	item := tx.db.deleteFromDatabase(&dbItem{key: key})
	if item == nil {
		return nil, ErrNotFound
	}
	// create a rollback entry if there has not been a deleteAll call.
	if tx.wc.rbKeys == nil {
		if _, ok := tx.wc.rollbackItems[string(key)]; !ok {
			tx.wc.rollbackItems[string(key)] = item
		}
	}

	return item.value, nil
}

// Match returns true if the specified key matches the pattern. This is a very
// simple pattern matcher where '*' matches on any number characters and '?'
// matches on any one character.
func Match(key, pattern []byte) bool {
	return match.Match(key, pattern)
}

// AscendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) AscendPrefixKeys(pattern []byte, iterator IterFunc) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	var tr *Trie
	tr = tx.db.keys
	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.iterCount++
		defer func() {
			tx.wc.iterCount--
		}()
	}
	iter := func(key []byte, val interface{}) bool {
		return iterator(key, val)
	}
	tr.PrefixSearch(pattern, iter)
	return nil
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(iterator IterFunc) error {
	return tx.AscendPrefixKeys(nil, iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// the results will be ordered by the item key.
func (tx *Tx) AscendLessThan(pivot []byte, iterator IterFunc) error {
	iter := func(key []byte, val interface{}) bool {
		if pivot != nil {
			if bytes.Compare(key, pivot) >= 0 {
				return false
			}
		}
		return iterator(key, val)
	}
	tx.AscendPrefixKeys(nil, iter)
	return nil
}

// AscendRange calls the iterator for every item in the database within
// the range [start, limit), until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
// Note: Only support start is nil or prefix of limit if limit is not nil
// If start != nil and limit == nil, only iter keys has start as common prefix
func (tx *Tx) AscendRange(start, limit []byte, iterator IterFunc) error {
	if start != nil && limit != nil && !bytes.HasPrefix(limit, start) {
		return errors.New("invalid range")
	}
	iter := func(key []byte, val interface{}) bool {
		if limit != nil {
			if bytes.Compare(key, limit) >= 0 {
				return false
			}
		}
		return iterator(key, val)
	}
	tx.AscendPrefixKeys(start, iter)
	return nil
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendEqual(pivot []byte, iterator IterFunc) error {
	return tx.AscendPrefixKeys(pivot, func(key []byte, value interface{}) bool {
		if bytes.Compare(key, pivot) != 0 {
			return false
		}
		return iterator(key, value)
	})
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.ReplaceOrInsert(item.Key(), item.Value())
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = &dbItem{key: item.Key(), value: prev.Property()}
	}

	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB) deleteFromDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.Delete(item.Key())
	if prev != nil {
		pdbi = &dbItem{key: item.Key(), value: prev.Property()}
	}
	return pdbi
}


// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must rollback.
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
func (db *DB) get(key []byte) interface {} {
	node, find := db.keys.Find(key)
	if !find {
		return nil
	}
	return node.Property()
}
