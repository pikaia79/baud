package master

import (
	"proto/metapb"
	"btree"
	"sync"
)

type Partition struct {
	*metapb.Partition

	//replGroup 	 uint32

	//for splitting & merging
	leftCh  *Partition
	rightCh *Partition
	parent  *Partition

	status string //serving, splitting, cleaning, etc.
}

type ReplicaGroup struct {
	id       uint32
	replicas []PartitionServer
}

type PartitionTree struct {
	btree 	btree.BTree
}

type PartitionServer struct {
	*metapb.PartitionServer

	cpu    int            `json:"-"`
	memory int                `json:"-"`
	disk   int                 `json:"-"`

	partition Partition   `json:"-"`
}

type PSCache struct {
	lock 	sync.RWMutex
	nodes   map[uint32]*PartitionServer
}

