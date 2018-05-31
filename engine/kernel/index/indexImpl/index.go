package indexImpl

import (
	"context"

	"github.com/tiglabs/baudengine/kernel"
	"github.com/tiglabs/baudengine/kernel/mapping"
	"github.com/tiglabs/baudengine/kernel/store/kvstore"
)

var RAFT_APPLY_ID []byte = []byte("Raft_apply_id")

var _ engine.Engine = &IndexDriver{}

type IndexDriver struct {
	store        kvstore.KVStore
	indexMapping mapping.IndexMapping
}

func NewIndexDriver(store kvstore.KVStore) *IndexDriver {
	return &IndexDriver{
		store: store,
	}
}

func (id *IndexDriver) NewSnapshot() (engine.Snapshot, error) {
	snap, err := id.store.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{snap: snap}, nil
}

// TODO clear store kv paris before apply snapshot
func (id *IndexDriver) ApplySnapshot(ctx context.Context, iter engine.Iterator) error {
	var batch kvstore.KVBatch
	count := 0
	for iter.Valid() {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if batch == nil {
			batch = id.store.NewKVBatch()
		}
		batch.Set(iter.Key(), iter.Value())
		count++
		if count%100 == 0 {
			err := id.store.ExecuteBatch(batch)
			if err != nil {
				return err
			}
			batch = nil
		}
		iter.Next()
	}
	if batch != nil {
		return id.store.ExecuteBatch(batch)
	}
	return nil
}


func (id *IndexDriver) NewWriteBatch() engine.Batch {
	return NewBatch(id.store)
}

