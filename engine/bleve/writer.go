package bleve

import (
	"context"
	"encoding/binary"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/document"
	"github.com/tiglabs/baudengine/engine"
)

func(w *Bleve) SetApplyID(applyID uint64) (err error) {
	batch := NewBatch(w.index)
	defer func() {
		if err == nil {
			err = batch.Commit()
		} else {
			batch.Rollback()
		}
	}()
	err = batch.SetApplyID(applyID)

	return
}

func (w *Bleve)AddDocument(ctx context.Context, docID engine.DOC_ID, doc interface{}) (err error) {
	batch := NewBatch(w.index)
	defer func() {
		if err == nil {
			err = batch.Commit()
		} else {
			batch.Rollback()
		}
	}()
	err = batch.AddDocument(ctx, docID, doc)
	return
}

func(w *Bleve) UpdateDocument(ctx context.Context, docID engine.DOC_ID, doc interface{}, upsert bool) (found bool, err error) {
	batch := NewBatch(w.index)
	defer func() {
		if err == nil {
			err = batch.Commit()
		} else {
			batch.Rollback()
		}
	}()
	return batch.UpdateDocument(ctx, docID, doc, upsert)
}

func(w *Bleve) DeleteDocument(ctx context.Context, docID engine.DOC_ID) (count int, err error) {
	batch := NewBatch(w.index)
	defer func() {
		if err == nil {
			err = batch.Commit()
		} else {
			batch.Rollback()
		}
	}()
	return batch.DeleteDocument(ctx, docID)
}

var _ engine.Batch = &Batch{}

type Batch struct {
	index bleve.Index
	batch *bleve.Batch
}

func NewBatch(index bleve.Index) *Batch {
	return &Batch{index: index, batch: index.NewBatch()}
}

func(b *Batch) SetApplyID(applyID uint64) error {
	if applyID > 0 {
		var buff [8]byte
		binary.BigEndian.PutUint64(buff[:], applyID)
		b.batch.SetInternal(RAFT_APPLY_ID, buff[:])
	}
	return nil
}

func (b *Batch)AddDocument(ctx context.Context, docID engine.DOC_ID, doc interface{}) error {
	return b.batch.Index(docID.ToString(), doc)
}

func(b *Batch) UpdateDocument(ctx context.Context, docID engine.DOC_ID, doc interface{}, upsert bool) (found bool, err error) {
	var _doc *document.Document
	var _index index.Index
	var reader index.IndexReader
	_index, _, err = b.index.Advanced()
	if err != nil {
		return
	}
	reader, err = _index.Reader()
	if err != nil {
		return
	}
	_doc, err = reader.Document(docID.ToString())
	if err != nil {
		return false, err
	}
	if _doc != nil {
		found = true
	}
	if !upsert && !found {
		return
	}
	err =  b.batch.Index(docID.ToString(), doc)
	return
}

func(b *Batch) DeleteDocument(ctx context.Context, docID engine.DOC_ID) (int, error) {
	var _doc *document.Document
	var _index index.Index
	var reader index.IndexReader
	var err error
	_index, _, err = b.index.Advanced()
	if err != nil {
		return 0, err
	}
	reader, err = _index.Reader()
	if err != nil {
		return 0, err
	}
	_doc, err = reader.Document(docID.ToString())
	if err != nil {
		return 0, err
	}
	if _doc == nil {
		return 0, nil
	}
	b.batch.Delete(docID.ToString())
	return 1, nil
}

func (b *Batch) Commit() error {
	return b.index.Batch(b.batch)
}

func (b *Batch) Rollback() error {
	b.batch.Reset()
	return nil
}