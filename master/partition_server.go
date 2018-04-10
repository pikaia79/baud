package master

import (
    "sync"
    "proto/metapb"
    "time"
)

const (
    DEFAULT_REPLICA_LIMIT_PER_PS = 1
)

type PartitionServer struct {
    *metapb.PartitionServer
    *metapb.ServerResource            `json:"-"`

    status        metapb.PSStatus
    lastHeartbeat time.Time
    replicaCache  *ReplicaCache
    propertyLock  sync.RWMutex
}

func (p *PartitionServer) isReplicaFull() bool {
    p.propertyLock.RLock()
    defer p.propertyLock.RUnlock()

    return p.replicaCache.count() == DEFAULT_REPLICA_LIMIT_PER_PS
}

type PSCache struct {
    lock 	sync.RWMutex
    nodes   map[uint32]*PartitionServer
}

func (c *PSCache) getAllPartitionServers() []*PartitionServer {
    c.lock.RLock()
    defer c.lock.RUnlock()

    nodes := make([]*PartitionServer, len(c.nodes))
    for _, ps := range(c.nodes) {
        nodes = append(nodes, ps)
    }

    return nodes
}


