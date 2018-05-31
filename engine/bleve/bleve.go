package bleve

import (
	"context"

	"github.com/blevesearch/bleve"
	"github.com/tiglabs/baudengine/engine"
)

var RAFT_APPLY_ID = []byte("_raft_apply_id")

var _ engine.Engine = &BleveEngine{}

type BleveEngine struct {
	driver bleve.Index
}

func(b *BleveEngine)NewWriteBatch() engine.Batch {
	return NewBatch(b.driver)
}

func (b *BleveEngine)NewSnapshot() (engine.Snapshot, error) {

}

func (b *BleveEngine)ApplySnapshot(ctx context.Context, iter engine.Iterator) error {

}


