package master

import (
    "sync"
    "proto/metapb"
)

type PartitionServer struct {
    *metapb.PartitionServer

    replicaCache  *ReplicaCache

    cpu    int            `json:"-"`
    memory int                `json:"-"`
    disk   int                 `json:"-"`
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

