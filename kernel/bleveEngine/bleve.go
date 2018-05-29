package bleveEngine

import (
	"github.com/tiglabs/baudengine/kernel"
	"context"
	"github.com/blevesearch/bleve"
)

var _ kernel.Engine = &BleveEngine{}

type BleveEngine struct {
	driver bleve.Index
}

func(b *BleveEngine)NewWriteBatch()kernel.Batch {
	b.driver.NewBatch()
}

func (b *BleveEngine)NewSnapshot() (kernel.Snapshot, error) {

}

func (b *BleveEngine)ApplySnapshot(ctx context.Context, iter kernel.Iterator) error {

}


