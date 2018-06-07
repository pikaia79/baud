package badgerdb

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/dgraph-io/badger"
	"github.com/rubenv/gorocksdb"
)

var _ store.KVWriter = &Writer{}

type Writer struct {
	db *gorocksdb.DB
	mo store.MergeOperator
}

func NewWriter(db *gorocksdb.DB, mo store.MergeOperator) *Writer {
	return &Writer{db: db, mo: mo}
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w.mo)
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) ([]byte, store.KVBatch, error) {
	return make([]byte, options.TotalBytes), w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(batch store.KVBatch) (err error) {

	emulatedBatch, ok := batch.(*store.EmulatedBatch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}

	tx := w.db.Write(true)
	// defer function to ensure that once started,
	// we either Commit tx or Rollback
	defer func() {
		// if nothing went wrong, commit
		if err == nil {
			// careful to catch error here too
			err = tx.Commit(nil)
		} else {
			// caller should see error that caused abort,
			// not success or failure of Rollback itself
			tx.Discard()
		}
	}()

	for k, mergeOps := range emulatedBatch.Merger.Merges {
		var existingVal []byte
		kb := []byte(k)
		existingVal, err = w.get(tx, kb)
		if err != nil {
			return
		}
		mergedVal, fullMergeOk := w.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			err = fmt.Errorf("merge operator returned failure")
			return
		}
		err = tx.Set(kb, mergedVal)
		if err != nil {
			return
		}
	}

	for _, op := range emulatedBatch.Ops {
		if op.V != nil {
			err = tx.Set(op.K, op.V)
			if err != nil {
				return
			}
		} else {
			err = tx.Delete(op.K)
			if err != nil {
				return
			}
		}
	}
	return
}

func (w *Writer) get(tx *badger.Txn, key []byte) ([]byte, error) {
	v, err := tx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return v.ValueCopy([]byte(nil))
}

func (w *Writer) Close() error {
	return nil
}