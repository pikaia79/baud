package btreeDB

import (
	"sync"
	"errors"
	"baud/util/match"
	"bytes"

	"github.com/google/btree"
	"golang.org/x/net/context"
)

type dbItem struct {
	key     []byte
	value   []byte
}

func (dbi *dbItem) Less(item btree.Item, ctx interface{}) bool {
	dbi2 := item.(*dbItem)

	return bytes.Compare(dbi.key, dbi2.key) < 0
}

type Tx struct {
	db       *DB    // the underlying database.
	writable bool            // when false mutable operations fail.
	funcd    bool            // when true Commit and Rollback panic.
	wc       *txWriteContext // context for writable transactions.
}

type txWriteContext struct {
	rbKeys *btree.BTree      // a tree of all item ordered by key

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

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	ErrTxIterating = errors.New("tx is iterating")
)

type DB struct {
	mu        sync.RWMutex
	keys      *btree.BTree      // a tree of all item ordered by key

	closed    bool              // set when the database has been closed
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
		tx.db.deleteFromDatabase(&dbItem{key: key})
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

func (tx *Tx) Set(key, value []byte) (preValue []byte,
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
			tx.wc.rollbackItems[key] = nil
		} else {
			// A previous item already exists in the database. Let's create a
			// rollback entry with the item as the value. We need to check the
			// map to see if there isn't already an item that matches the
			// same key.
			if _, ok := tx.wc.rollbackItems[key]; !ok {
				tx.wc.rollbackItems[key] = prev
			}
			preValue, replaced = prev.value, true
		}
	}
	return preValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist then ErrNotFound is returned.
func (tx *Tx) Get(key []byte) (val []byte, err error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}

	item := tx.db.get(key)
	if item == nil {
		// The item does not exists
		return nil, ErrNotFound
	}
	return item.value, nil
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// This operation is not allowed during iterations such as Ascend* & Descend*.
func (tx *Tx) Delete(key []byte) (val []byte, err error) {
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
		if _, ok := tx.wc.rollbackItems[key]; !ok {
			tx.wc.rollbackItems[key] = item
		}
	}

	return item.value, nil
}

func (tx *Tx) NewCursor() *Cursor {
	return &Cursor{tx: tx}
}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, start, stop []byte,
iterator func(key, value []byte) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.value)
	}
	var tr *btree.BTree
	tr = tx.db.keys
	// create some limit items
	var itemA, itemB *dbItem
	if gt || lt {
		itemA = &dbItem{key: start}
		itemB = &dbItem{key: stop}
	}
	// execute the scan on the underlying tree.
	if tx.wc != nil {
		tx.wc.iterCount++
		defer func() {
			tx.wc.iterCount--
		}()
	}
	if desc {
		if gt {
			if lt {
				tr.DescendRange(itemA, itemB, iter)
			} else {
				tr.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			tr.DescendLessOrEqual(itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(itemA, itemB, iter)
			} else {
				tr.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			tr.AscendLessThan(itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

// Match returns true if the specified key matches the pattern. This is a very
// simple pattern matcher where '*' matches on any number characters and '?'
// matches on any one character.
func Match(key, pattern []byte) bool {
	return match.Match(key, pattern)
}

// AscendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) AscendKeys(pattern []byte,
iterator func(key, value []byte) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if string(pattern) == "*" {
			return tx.Ascend(iterator)
		}
		return tx.Ascend(func(key, value []byte) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.AscendGreaterOrEqual(min, func(key, value []byte) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys allows for iterating through keys based on the specified pattern.
func (tx *Tx) DescendKeys(pattern []byte,
iterator func(key, value []byte) bool) error {
	if pattern == nil {
		return nil
	}
	if pattern[0] == '*' {
		if string(pattern) == "*" {
			return tx.Descend(iterator)
		}
		return tx.Descend(func(key, value []byte) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.DescendLessOrEqual(max, func(key, value []byte) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(iterator func(key, value []byte) bool) error {
	return tx.scan(false, false, false, nil, nil, iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// The results will be ordered by the item key.
func (tx *Tx) AscendGreaterOrEqual(pivot []byte,
iterator func(key, value []byte) bool) error {
	return tx.scan(false, true, false, pivot, nil, iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// the results will be ordered by the item key.
func (tx *Tx) AscendLessThan(pivot []byte,
iterator func(key, value []byte) bool) error {
	return tx.scan(false, false, true, pivot, nil, iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(greaterOrEqual, lessThan []byte,
iterator func(key, value []byte) bool) error {
	return tx.scan(false, true, true, greaterOrEqual, lessThan, iterator)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// The results will be ordered by the item key.
func (tx *Tx) Descend(iterator func(key, value []byte) bool) error {
	return tx.scan(true, false, false, nil, nil, iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(pivot []byte,
iterator func(key, value []byte) bool) error {
	return tx.scan(true, true, false, pivot, nil, iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendLessOrEqual(pivot string,
iterator func(key, value string) bool) error {
	return tx.scan(true, false, true, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendRange(lessOrEqual, greaterThan []byte,
iterator func(key, value []byte) bool) error {
	return tx.scan(true, true, true, lessOrEqual, greaterThan, iterator)
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendEqual(pivot []byte,
iterator func(key, value []byte) bool) error {
	return tx.AscendGreaterOrEqual(pivot, func(_key, _value []byte) bool {
		if bytes.Compare(_key, pivot) != 0 {
			return false
		}
		return iterator(_key, _value)
	})
}

// DescendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false
// The results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendEqual(pivot []byte,
iterator func(key, value []byte) bool) error {
	return tx.DescendLessOrEqual(pivot, func(_key, _value []byte) bool {
		if bytes.Compare(_key, pivot) != 0 {
			return false
		}
		return iterator(_key, _value)
	})
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.ReplaceOrInsert(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*dbItem)
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
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*dbItem)
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
func (db *DB) get(key string) *dbItem {
	item := db.keys.Get(&dbItem{key: key})
	if item != nil {
		return item.(*dbItem)
	}
	return nil
}

type KV struct {
	key     []byte
	value   []byte
}

type Cursor struct {
	tx *Tx

	ctx      context.Context
	cancel   context.CancelFunc
	running  bool
	wg       sync.WaitGroup
	pipe     chan *KV
	firstKV  *KV
	kv       *KV
}

func (c *Cursor) iter() {
	c.wg.Add(1)
	go func()
}

func (c *Cursor) Seek(seek []byte) (key []byte, value []byte){
	// 已经存在一个迭代器了,先 close
	if c.running {
		c.wg.Wait()
		close(c.pipe)
		c.pipe = make(chan *KV, 1000)
		c.firstKV = nil
		c.kv = nil
		c.running = false
	}
	c.tx.AscendGreaterOrEqual(seek, func(k, v []byte) bool {
		if c.firstKV == nil {
			c.firstKV = &KV{key: k, value: v}
		} else {
			select {
			case <-c.ctx.Done():
				return false
			case c.pipe <-&KV{key: k, value: v}:
				return true
			}
		}
	})
}

func (c *Cursor) First() (key []byte, value []byte) {
	// 还没有开始迭代
	if c.firstKV == nil {
		return c.firstKV.key, c.firstKV.value
	}
	return nil, nil
}

func (c *Cursor) Next() (key []byte, value []byte) {
	select {
	case kv, ok :=<-c.pipe:
	    if !ok {
		    return nil, nil
	    }
		return kv.key, kv.value
	default:
		return nil, nil
	}
	return nil, nil
}