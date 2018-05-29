package bleveEngine

import (
	"context"
	"github.com/tiglabs/baudengine/kernel"
	"github.com/blevesearch/bleve"
)

func(w *BleveEngine) SetApplyID(uint64) error {

}

func (w *BleveEngine)AddDocument(ctx context.Context, docID kernel.DOC_ID, doc interface{}) error {

}

func(w *BleveEngine) UpdateDocument(ctx context.Context, docID kernel.DOC_ID, doc interface{}, upsert bool) (found bool, err error) {

}

func(w *BleveEngine) DeleteDocument(ctx context.Context, docID kernel.DOC_ID) (int, error) {

}

type Batch struct {
	batch bleve.Batch
}

func(b *Batch) SetApplyID(uint64) error {

}

func (b *Batch)AddDocument(ctx context.Context, docID kernel.DOC_ID, doc interface{}) error {
	return b.batch.Index(docID.ToString(), doc)
}

func(b *Batch) UpdateDocument(ctx context.Context, docID kernel.DOC_ID, doc interface{}, upsert bool) (found bool, err error) {
	return b.batch.Index(docID.ToString(), doc)
}

func(b *Batch) DeleteDocument(ctx context.Context, docID kernel.DOC_ID) (int, error) {

}