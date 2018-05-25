package topo

import (
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/tiglabs/baudengine/proto/masterpb"
)

type PartitionTopo struct {
    version Version
    *metapb.Partition
}

type PartitionInfoTopo struct {
    version Version
    *masterpb.PartitionInfo
}
