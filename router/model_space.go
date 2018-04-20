package router

import (
	"github.com/tiglabs/baud/proto/metapb"
	"sync"
)

type Space struct {
	meta metapb.Space
	parent *DB
	partitions []*Partition
	lock     sync.RWMutex
}

func (space *Space) GetPartition(id uint32) *Partition {
	space.lock.RLock()
	defer space.lock.RUnlock()

	return nil
}