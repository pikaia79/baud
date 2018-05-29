package index

import (
	"encoding/binary"
	"context"
	"fmt"
	"errors"

	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
	"github.com/tiglabs/baudengine/kernel"
	"github.com/tiglabs/baudengine/kernel/registry"
	"github.com/tiglabs/baudengine/util/encoding"
	"github.com/tiglabs/baudengine/kernel/analysis"
)

func (w *IndexDriver) SetApplyID(applyID uint64) error {
	if applyID > 0 {
		var buff [8]byte
		binary.BigEndian.PutUint64(buff[:], applyID)
		return w.store.Put(RAFT_APPLY_ID, buff[:])
	}
	return nil
}

func (w *IndexDriver) AddDocument(ctx context.Context, doc *pspb.Document) error {
	return NewBatch(w.store).addDocument(ctx, doc, true)
}

func (w *IndexDriver) UpdateDocument(ctx context.Context, doc *pspb.Document, upsert bool) (found bool, err error) {
	return NewBatch(w.store).updateDocument(ctx, doc, upsert, true)
}

func (w *IndexDriver) DeleteDocument(ctx context.Context, docID metapb.Key) (int, error) {
	return NewBatch(w.store).deleteDocument(ctx, docID, true)
}

type Batch struct {
	batch   kvstore.KVBatch
	tx kvstore.Transaction
	store   kvstore.KVStore
}

var _ engine.Batch = &Batch{}

func NewBatch(store kvstore.KVStore) *Batch {
	return &Batch{store: store, batch: store.NewKVBatch()}
}

func (b *Batch) SetApplyID(applyID uint64) error {
	if applyID > 0 {
		var buff [8]byte
		binary.BigEndian.PutUint64(buff[:], applyID)
		b.batch.Set(RAFT_APPLY_ID, buff[:])
	}
	return nil
}

func (b *Batch) AddDocument(ctx context.Context, doc *pspb.Document) error {
	return b.addDocument(ctx, doc, false)
}

func (b *Batch) addDocument(ctx context.Context, doc *pspb.Document, forceCommit bool) error {
	// todo check doc ???
	// encode field
	for _, field := range doc.Fields {
		fk, fv, err := encodeStoreField(doc.Id, &field)
		if err != nil {
			return err
		}
        b.batch.Set(fk, fv)
		// analysis field value
		if field.Desc.Tokenized {
			analyzer := registry.GetAnalyzer(field.Desc.Analyzer)
			if analyzer == nil {
				return fmt.Errorf("unknown analyzer %s", field.Desc.Analyzer)
			}
            data := []byte(field.Data)
            for len(data) > 0 {
            	var fData []byte
            	data, fData, err = encoding.DecodeBytesValue(data)
            	if err != nil {
            		return err
	            }
	            tokens := analyzer.Analyze(fData)
	            // TODO position gap
	            var includeTermVectors bool
	            switch field.Desc.IndexOption {
	            case pspb.IndexOption_DOCS, pspb.IndexOption_DOCS_FREQ:
		            includeTermVectors = false
	            case pspb.IndexOption_DOCS_FREQ_POSITION, pspb.IndexOption_DOCS_FREQ_POSITION_OFFSET:
		            includeTermVectors = true
	            }
	            tokenFreq := analysis.TokenFrequency(tokens, includeTermVectors)
	            var terms [][]byte
	            terms = make([][]byte, 0, len(tokenFreq))
	            for _, tokenF := range tokenFreq {
	            	indexKey, indexRow, err := encodeIndex(doc.Id, field.Id, tokenF.Term, tokenF.Frequency())
	            	if err != nil {
	            		return err
		            }
		            b.batch.Set(indexKey, indexRow)
	            	if includeTermVectors {
	            		for _, pos := range tokenF.Locations {
				            indexPosKey, indexPosRow, err := encodeIndexPosition(doc.Id, field.Id, tokenF.Term, pos.Position, pos.Start, pos.End)
				            if err != nil {
					            return err
				            }
				            b.batch.Set(indexPosKey, indexPosRow)
			            }
		            }
		            terms = append(terms, tokenF.Term)
	            }
	            fieldTermKey, fieldTermValue, err := encodeFieldTermAbstract(doc.Id, field.Id, terms)
	            if err != nil {
	            	return err
	            }
	            b.batch.Set(fieldTermKey, fieldTermValue)
            }
		}
	}
	if forceCommit {
		return b.Commit()
	}
	return nil
}

func (b *Batch) UpdateDocument(ctx context.Context, doc *pspb.Document, upsert bool) (found bool, err error) {
	return b.updateDocument(ctx, doc, upsert, false)
}

func (b *Batch) updateDocument(ctx context.Context, doc *pspb.Document, upsert bool, forceCommit bool) (found bool, err error) {
	// step 1. find doc
	if isDocExist(b.store, []byte(doc.Id)) {
		found = true
	}
	if !found && !upsert {
		return
	}
	if found {
		_, err = b.deleteDocument(ctx, doc.Id, false)
		if err != nil {
			return
		}
	}
	err = b.addDocument(ctx, doc, false)
	if forceCommit {
		err = b.Commit()
	}
	return
}

func (b *Batch) DeleteDocument(ctx context.Context, docID metapb.Key) (int, error) {
	return b.deleteDocument(ctx, docID, false)
}

func (b *Batch) deleteDocument(ctx context.Context, docID metapb.Key, forceCommit bool) (int, error) {
	prefixDocKey := encodeStoreFieldKey(docID, 0)
	fieldIter := b.store.PrefixIterator(prefixDocKey)
	if fieldIter == nil {
		return 0, errors.New("store driver error")
	}
	count := 0
	for fieldIter.Valid() {
		// delete field
		b.batch.Delete(fieldIter.Key())
		fieldIter.Next()
		count++
	}
	defer fieldIter.Close()
	// no document for the docID
	if count == 0 {
		return 0, nil
	}
	count = 1
	prefixFieldTermKey := encodeFieldTermAbstractKey([]byte(docID), 0)
	fieldTermIter := b.store.PrefixIterator(prefixFieldTermKey)
	if fieldTermIter == nil {
		return 0, errors.New("store driver error")
	}
	defer fieldTermIter.Close()
	for fieldTermIter.Valid() {
		_, fieldId, err := decodeFieldTermAbstractKey(fieldTermIter.Key())
		if err != nil {
			return 0, err
		}
		terms, err := decodeFieldTermAbstractValue(fieldTermIter.Value())
		if err != nil {
			return 0, err
		}
		// delete field terms
		b.batch.Delete(fieldTermIter.Key())
		for _, term := range terms {
			// delete term index
			b.batch.Delete(encodeIndexKey(docID, fieldId, term))
			prefixTermPosKey := encodeIndexPositionKey(docID, fieldId, term, 0)
			termPosIter := b.store.PrefixIterator(prefixTermPosKey)
			if termPosIter == nil {
				return 0, errors.New("store driver error")
			}
			for termPosIter.Valid() {
				// delete term position
				b.batch.Delete(termPosIter.Key())
				termPosIter.Next()
			}
			termPosIter.Close()
		}
		fieldTermIter.Next()
	}
	return count, b.Commit()
}

func (b *Batch) Commit() error {
	if b.tx == nil {
		tx, err := b.store.NewTransaction(true)
		if err != nil {
			return err
		}
		b.tx = tx
	}
	// TODO remove batch, replace with tx when add/update/delete document
	for _, op := range b.batch.Operations() {
		// TODO in badger driver, the tx may full
		if op.Value() == nil {
			b.tx.Delete(op.Key())
		} else {
			b.tx.Put(op.Key(), op.Value())
		}
	}
	return b.tx.Commit()
}

func (b *Batch) Rollback() error {
	if b.tx != nil {
		return b.tx.Rollback()
	}
	return nil
}

func isDocExist(store kvstore.KVStore, docID []byte) (bool) {
	key := encodeStoreFieldKey(docID, 0)
	iter := store.PrefixIterator(key)
	if iter == nil {
		return false
	}
	defer iter.Close()
	return iter.Valid()
}
