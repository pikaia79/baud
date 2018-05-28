package topo

import (
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/tiglabs/baudengine/util/log"
    "context"
    "errors"
    "flag"
    "github.com/tiglabs/baudengine/proto/masterpb"
    "time"
)

const (
    // Path components
    ZonesPath            = "zones"
    dbsPath              = "dbs"
    spacesPath           = "spaces"
    partitionsPath       = "partitions"
    partitionServersPath = "servers"
    tasksPath            = "tasks"

    // Filenames for all object types.
    ZoneTopoFile            = "zone_info"
    DBTopoFile              = "db_info"
    SpaceTopoFile           = "space_info"
    PartitionServerTopoFile = "ps_info"
    ReplicaTopoFile         = "replica_info"
    PartitionTopoFile       = "partition_info"
    partitionGroupTopoFile  = "partition_group_info"
    TaskTopoFile            = "task_info"
)

var (
    topoImplementation    = flag.String("topo_implementation", "etcd3", "the topology implementation to use")
    topoGlobalServerAddrs = flag.String("topo_global_server_addrs", "", "topo global server addresses")
    topoGlobalRootDir     = flag.String("topo_global_root_dir", "/",
            "the path of the global topology data in the global topology server")
)

var (
    // ErrNodeExists is returned by functions to specify the
    // requested resource already exists.
    ErrNodeExists = errors.New("node already exists")

    // ErrNoNode is returned by functions to specify the requested
    // resource does not exist.
    ErrNoNode = errors.New("node doesn't exist")

    // ErrNotEmpty is returned by functions to specify a child of the
    // resource is still present and prevents the action from completing.
    ErrNotEmpty = errors.New("node not empty")

    // ErrTimeout is returned by functions that wait for a result
    // when the timeout value is reached.
    ErrTimeout = errors.New("deadline exceeded")

    // ErrInterrupted is returned by functions that wait for a result
    // when they are interrupted.
    ErrInterrupted = errors.New("interrupted")

    // ErrBadVersion is returned by an update function that
    // failed to update the data because the version was different
    ErrBadVersion = errors.New("bad node version")

    // ErrPartialResult is returned by a function that could only
    // get a subset of its results
    ErrPartialResult = errors.New("partial result")

    // ErrNoUpdateNeeded can be returned by an 'UpdateFields' method
    // to skip any update.
    ErrNoUpdateNeeded = errors.New("no update needed")

    ErrZoneNotExists = errors.New("zone not exists")
)

type Impl interface {

    GetAllZones(ctx context.Context) ([]*ZoneTopo, error)
    GetZone(ctx context.Context, zoneName string) (*ZoneTopo, error)
    AddZone(ctx context.Context, zone *metapb.Zone) (*ZoneTopo, error)
    DeleteZone(ctx context.Context, zone *ZoneTopo) error

    GetAllDBs(ctx context.Context) ([]*DBTopo, error)
    GetDB(ctx context.Context, dbId metapb.DBID) (*DBTopo, error)
    AddDB(ctx context.Context, db *metapb.DB) (*DBTopo, error)
    UpdateDB(ctx context.Context, db *DBTopo) error
    DeleteDB(ctx context.Context, db *DBTopo) error

    GetAllSpaces(ctx context.Context) ([]*SpaceTopo, error)
    GetSpace(ctx context.Context, dbId metapb.DBID, spaceId metapb.SpaceID) (*SpaceTopo, error)
    AddSpace(ctx context.Context, space *metapb.Space, partitions []*metapb.Partition) (*SpaceTopo,
            []*PartitionTopo, error)
    UpdateSpace(ctx context.Context, space *SpaceTopo) error
    DeleteSpace(ctx context.Context, space *SpaceTopo) error

    GetAllPartitions(ctx context.Context) ([]*PartitionTopo, error)
    GetPartition(ctx context.Context, partitionId metapb.PartitionID) (*PartitionTopo, error)
    UpdatePartition(ctx context.Context, partition *PartitionTopo) error
    DeletePartition(ctx context.Context, partition *PartitionTopo) error

    GetAllPsByZone(ctx context.Context, zoneName string) ([]*PsTopo, error)
    GetPsByZone(ctx context.Context, zoneName string, psId metapb.NodeID) (*PsTopo, error)
    AddPsByZone(ctx context.Context, zoneName string, node *metapb.Node) (*PsTopo, error)
    UpdatePsByZone(ctx context.Context, zoneName string, ps *PsTopo) error
    DeletePsByZone(ctx context.Context, zoneName string, ps *PsTopo) error

    SetZonesForPartition(ctx context.Context, partitionId metapb.PartitionID, zones []string) error
    GetZonesForPartition(ctx context.Context, partitionId metapb.PartitionID) ([]string, error)

    GetPartitionInfoByZone(ctx context.Context, zoneName string,
            partitionId metapb.PartitionID) (*masterpb.PartitionInfo, error)
    SetPartitionInfoByZone(ctx context.Context, zoneName string, partitionInfo *masterpb.PartitionInfo) error
    // SetPartitionLeaderByZone(ctx context.Context, zoneName string,
    //        partitionId *metapb.PartitionID, leaderReplicaId metapb.ReplicaID) error


    GetPartitionsOnPsByZone(ctx context.Context, zoneName string, psId metapb.NodeID) ([]*PartitionTopo, error)
    SetPartitionsOnPSByZone(ctx context.Context, zoneName string, psId metapb.NodeID,
            partitions []*metapb.Partition) error

    SetTask(ctx context.Context, task metapb.Task, timeout time.Duration) error
    GetTask(ctx context.Context, taskName string, taskId string) (*metapb.Task, error)

    NewMasterParticipation(zone, id string) (MasterParticipation, error)

    GenerateNewId(ctx context.Context) (uint64, error)
}

type TopoServer struct {
    backend Backend
}

func (s *TopoServer) Close() {
    if s.backend != nil {
       s.backend.Close()
       s.backend = nil
    }
}

// Factory is a factory method to create Impl objects.
type Factory func(serverAddr, root string) (Backend, error)

var (
    factories = make(map[string]Factory)
)

func RegisterFactory(name string, factory Factory) {
    if factories[name] != nil {
        log.Error("Duplicate topo.Factory registration for %v", name)
    }
    factories[name] = factory
}

func OpenServer(implementation, globalServerAddrs, globalRootDir string) (*TopoServer, error) {
    factory, ok := factories[implementation]
    if !ok {
        log.Error("invalid implementation[%s]", implementation)
        return nil, ErrNoNode
    }

    backend, err := factory(globalServerAddrs, globalRootDir)
    if err != nil {
       log.Error("Fail to create etcd3 server. err[%v]", err)
       return nil, err
    }
    return &TopoServer{
        backend: backend,
    }, nil
}

func Open() *TopoServer {
    server, err := OpenServer(*topoImplementation, *topoGlobalServerAddrs, *topoGlobalRootDir)
    if err != nil {
        log.Fatal("Fail to create topo server with %s. err[%v]", *topoImplementation, err)
    }

    return server
}

