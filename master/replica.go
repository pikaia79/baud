package master

import (
    "sync"
    "time"
	"proto/metapb"
)

type Replica struct {
	*metapb.Replica

    lastHeartbeat   time.Time
	propertyLock    sync.RWMutex
}

type ReplicaCache struct {
    lock sync.RWMutex
    replicas map[uint32]*Replica
}

func (c *ReplicaCache) count() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return len(c.replicas)
}