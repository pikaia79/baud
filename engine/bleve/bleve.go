package bleve

import (
	"context"

	"github.com/blevesearch/bleve"
	"github.com/tiglabs/baudengine/engine"
	"github.com/blevesearch/bleve/index/store"
)

var RAFT_APPLY_ID = []byte("_raft_apply_id")

var _ engine.Engine = &Bleve{}

type Bleve struct {
	index bleve.Index
}

func(b *Bleve)NewWriteBatch() engine.Batch {
	return NewBatch(b.index)
}

func (b *Bleve)NewSnapshot() (engine.Snapshot, error) {
	_, store, err := b.index.Advanced()
	if err != nil {
		return nil, err
	}
	reader, err := store.Reader()
	if err != nil {
		return nil, err
	}
	return &Snapshot{reader: reader}, nil
}

func (b *Bleve)ApplySnapshot(ctx context.Context, iter engine.Iterator) error {
	_, _store, err := b.index.Advanced()
	if err != nil {
		return err
	}
	writer, err := _store.Writer()
	if err != nil {
		return err
	}
	var batch store.KVBatch
	var count int
	for iter.Valid() {
		if batch == nil {
			batch = store.NewEmulatedBatch(nil)
		}
		batch.Set(iter.Key(), iter.Value())
		count++
		if count % 100 == 0 {
			err = writer.ExecuteBatch(batch)
			if err != nil {
				return err
			}
			batch = nil
		}
	}
	if batch != nil {
		return writer.ExecuteBatch(batch)
	}
	return nil
}


