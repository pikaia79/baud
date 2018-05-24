package topo

import (
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/tiglabs/baudengine/proto/masterpb"
    "github.com/tiglabs/baudengine/util/log"
    "context"
    "errors"
    "flag"
)

const (
    // Path components
    zonesPath     = "zones"
    keyspacesPath = "keyspaces"
    shardsPath    = "shards"
    tabletsPath   = "tablets"

    // Filenames for all object types.
    ZoneInfoFile = "ZoneInfo"
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
)

type Impl interface {
   // Backend

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

