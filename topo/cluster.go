package topo

import (
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/tiglabs/baudengine/proto/masterpb"
    "context"
)

var (
    topoImplementation = "etcd3"

    globalZone = "global"
    globalAddr = "127.0.0.1:9301"

    zoneName   = []string{"myzone2", "myzone3"}
    zoneAddr   = []string{"127.0.0.1:9302", "127.0.0.1:9303"}
)

type Impl interface {
    GetAllZones(ctx context.Context) ([]*metapb.Zone, error)
    GetZone(ctx context.Context, zoneName string) (*metapb.Zone, error)
    AddZone(ctx context.Context, zone *metapb.Zone) error
    DeleteZone(ctx context.Context, zoneName string) error

    GetAllDBs(ctx context.Context) ([]*metapb.DB, error)
    GetDB(ctx context.Context, dbId metapb.DBID) (*metapb.DB, error)
    AddDB(ctx context.Context, db *metapb.DB) error
    UpdateDB(ctx context.Context, db *metapb.DB) error
    DeleteDB(ctx context.Context, dbId metapb.DBID) error

    GetAllSpaces(ctx context.Context) ([]*metapb.Space, error)
    GetSpace(ctx context.Context, spaceId metapb.SpaceID) (*metapb.Space, error)
    AddSpace(ctx context.Context, space *metapb.Space, partitions []*metapb.Partition) error
    UpdateSpace(ctx context.Context, space *metapb.Space) error
    DeleteSpace(ctx context.Context, spaceId metapb.SpaceID) error

    GetAllPartition(ctx context.Context) ([]*metapb.Partition, error)
    GetPartition(ctx context.Context, partitionId metapb.PartitionID) (*metapb.Partition, error)
    UpdatePartition(ctx context.Context, partition *metapb.Partition) error
    DeletePartition(ctx context.Context, partitionId metapb.PartitionID) error

    GetAllPsByZone(ctx context.Context, zoneName string) ([]*metapb.Node, error)
    GetPsByZone(ctx context.Context, zoneName string, nodeId metapb.NodeID) (*metapb.Node, error)
    AddPsByZone(ctx context.Context, zoneName string, node *metapb.Node) error
    UpdatePsByZone(ctx context.Context, zoneName string, ps *metapb.Node) error
    DeletePsByZone(ctx context.Context, zoneName string, nodeId metapb.NodeID) error

    // GetZoneForPartition(ctx context.Context, partitionId metapb.PartitionID) ([]*metapb.Zone, error)
    // GetRoute
    GetPartitionInfoByZone(ctx context.Context, zoneName string,
            partitionId *metapb.PartitionID) (*masterpb.PartitionInfo, error)

    // PsRegister
    GetPartitionsOnPsByZone(ctx context.Context, zoneName string, nodeId metapb.NodeID) ([]*metapb.Partition, error)

    // PsHeartbeat
    SetPartitionInfoByZone(ctx context.Context, zoneName string, partition *masterpb.PartitionInfo) error
    SetPartitionLeaderByZone(ctx context.Context, zoneName string,
            partitionId *metapb.PartitionID, leaderReplicaId metapb.ReplicaID) error

    GetGMLeaderAsync(ctx context.Context) <-chan *masterpb.GMaster
    GetGMLeaderSync(ctx context.Context) *masterpb.GMaster

    GetZMLeaderAsync(ctx context.Context, zoneName string) <-chan *masterpb.ZMaster
    GetZMLeaderSync(ctx context.Context, zoneName string) *masterpb.ZMaster

    GenerateNewId(ctx context.Context)
}

type TopoServer struct {
    Impl
}

func NewTopoServer() *TopoServer {
    return nil
}

