package router

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"sort"
	"sync"
	"github.com/tiglabs/baudengine/util/log"
)

type Space struct {
	meta       metapb.Space
	parent     *DB
	partitions []*Partition
	lock       sync.RWMutex
}

func NewSpace(parent *DB, meta metapb.Space) *Space {
	if str, err := json.Marshal(meta); err == nil {
		log.Debug("NewSpace(): %s", string(str))
	}
	return &Space{meta: meta, parent: parent}
}

func (space *Space) GetPartition(slotId metapb.SlotID) *Partition {
	partition, _ := space.getPartition(slotId)
	if partition == nil {
		routes := space.parent.masterClient.GetRoute(space.meta.DB, space.meta.ID, slotId)
		space.addRoutes(routes)
		partition, _ = space.getPartition(slotId)
	}
	if partition == nil {
		panic(errors.Errorf("cannot get partition for slot %d", slotId))
	}
	return partition
}

func (space *Space) getPartition(slotId metapb.SlotID) (*Partition, int) {
	space.lock.RLock()
	defer space.lock.RUnlock()

	pos := sort.Search(len(space.partitions), func(i int) bool {
		return space.partitions[i].meta.EndSlot >= slotId
	})
	if pos >= len(space.partitions) || slotId < space.partitions[pos].meta.StartSlot {
		return nil, -1
	}
	return space.partitions[pos], pos
}

func (space *Space) addRoutes(routes []masterpb.Route) {
	space.lock.Lock()
	defer space.lock.Unlock()

	for _, route := range routes {
		pos := sort.Search(len(space.partitions), func(i int) bool {
			return space.partitions[i].meta.EndSlot >= route.StartSlot
		})
		newPartition := NewPartition(space, route)
		if pos >= len(space.partitions) {
			space.partitions = append(space.partitions, newPartition)
		} else if space.partitions[pos].meta.EndSlot >= route.StartSlot {
			space.partitions = append(space.partitions[:pos], append([]*Partition{newPartition}, space.partitions[pos:]...)...)
		}
	}
}

func (space *Space) GetKeyField() string {
	return space.meta.KeyPolicy.KeyField
}

func (space *Space) Delete(partition metapb.Partition) {
	_, pos := space.getPartition(partition.StartSlot)
	if pos >= 0 {
		space.lock.Lock()
		defer space.lock.Unlock()

		space.partitions = append(space.partitions[:pos], space.partitions[pos + 1:]...)
	}
}
