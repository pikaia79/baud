package master

import "sync"

type Replica struct {
    id          uint32
    partitionId uint32
}

type ReplicaCache struct {
    lock sync.RWMutex
    replicas map[uint32]*Replica
}