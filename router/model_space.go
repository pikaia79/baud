package router

import (
	"github.com/pkg/errors"
	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"sort"
	"sync"
)

type Space struct {
	meta       metapb.Space
	parent     *DB
	partitions []*Partition
	lock       sync.RWMutex
}

func NewSpace(parent *DB, meta metapb.Space) *Space {
	return &Space{meta: meta, parent: parent}
}

func (space *Space) GetPartition(slotId metapb.SlotID) *Partition {
	partition := space.getPartition(slotId)
	if partition == nil {
		routes := space.parent.masterClient.GetRoute(space.meta.DB, space.meta.ID, slotId)
		space.addRoutes(routes)
		partition = space.getPartition(slotId)
	}
	if partition == nil {
		panic(errors.Errorf("cannot get partition for slot %d", slotId))
	}
	return partition
}

func (space *Space) getPartition(slotId metapb.SlotID) *Partition {
	space.lock.RLock()
	defer space.lock.RUnlock()

	pos := sort.Search(len(space.partitions), func(i int) bool {
		return space.partitions[i].meta.EndSlot >= slotId
	})
	if pos >= len(space.partitions) || slotId < space.partitions[pos].meta.StartSlot {
		return nil
	}
	return space.partitions[pos]
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
