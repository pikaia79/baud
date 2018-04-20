package master

import (
    "google.golang.org/grpc"
    "golang.org/x/net/context"
    "google.golang.org/grpc/reflection"
    "net"
    "util/log"
    "github.com/tiglabs/baud/proto/metapb"
    "github.com/tiglabs/baud/proto/masterpb"
)

type RpcServer struct {
    config     *Config
    grpcServer *grpc.Server

    cluster *Cluster
}

func NewRpcServer(config *Config, cluster *Cluster) *RpcServer {
    server := new(RpcServer)
    server.config = config
    server.cluster = cluster

    s := grpc.NewServer()
    masterpb.RegisterMasterRpcServer(s, server)
    reflection.Register(s)
    server.grpcServer = s

    return server
}

func (s *RpcServer) Start() error {
    l, err := net.Listen("tcp", s.config.rpcServerAddr)
    if err != nil {
        log.Error("rpc server listen error[%v]", err)
        return err
    }

    go func() {
        if err := s.grpcServer.Serve(l); err != nil {
            log.Error("grpc server serve error[%v]", err)
        }
    }()

    return nil
}

func (s *RpcServer) Close() {
    if s.grpcServer != nil {
        s.grpcServer.GracefulStop()
    }
}

func (s *RpcServer) GetRoute(ctx context.Context,
    request *masterpb.GetRouteRequest) (*masterpb.GetRouteResponse, error) {
    return nil, nil
}

func (s *RpcServer) GetDB(ctx context.Context, req *masterpb.GetDBRequest) (*masterpb.GetDBResponse, error) {
    return nil, nil
}

func (s *RpcServer) GetSpace(ctx context.Context, req *masterpb.GetSpaceRequest) (*masterpb.GetSpaceResponse, error) {
    return nil, nil
}

func (s *RpcServer) PSRegister(ctx context.Context,
    request *masterpb.PSRegisterRequest) (*masterpb.PSRegisterResponse, error) {
    resp := new(masterpb.PSRegisterResponse)

    nodeId := request.NodeID

    if nodeId == 0 {
        // this is a new ps unregistered never, distribute new psid to it
        ps, err := NewPartitionServer(request.Ip)
        if err != nil {
            resp.ResponseHeader = makeRpcRespHeader(err)
            return resp, nil
        }
        ps.persistent(s.cluster.store)

        ps.status = PS_REGISTERED
        s.cluster.psCache.addServer(ps)

        resp.ResponseHeader = makeRpcRespHeader(ErrSuc)
        resp.NodeID = ps.ID
        return resp, nil
    }

    // use nodeid reserved by ps to recognize same one ps
    ps := s.cluster.psCache.findServerById(nodeId)
    if ps == nil {
        // illegal ps will register
        log.Warn("Can not find nodeid[%v] in master.", nodeId)
        resp.ResponseHeader = makeRpcRespHeader(ErrPSNotExists)
        return resp, nil
    }

    // old ps rebooted
    ps.changeStatus(PS_REGISTERED)

    resp.ResponseHeader = makeRpcRespHeader(ErrSuc)
    resp.NodeID = ps.ID
    resp.Partitions = ps.partitionCache.getAllMetaPartitions()

    return resp, nil
}

func (s *RpcServer) PSHeartbeat(ctx context.Context,
    request *masterpb.PSHeartbeatRequest) (*masterpb.PSHeartbeatResponse, error) {
    resp := new(masterpb.PSHeartbeatResponse)

    psId := request.NodeID
    ps := s.cluster.psCache.findServerById(psId)
    if ps == nil {
        log.Error("ps heartbeat received invalid psid[%v]", psId)
        resp.ResponseHeader = makeRpcRespHeader(ErrPSNotExists)
        return resp, nil
    }

    partitionInfos := request.Partitions
    if partitionInfos == nil {
        resp.ResponseHeader = makeRpcRespHeader(ErrSuc)
        return resp, nil
    }

    for _, partitionInfo := range partitionInfos {
        partitionId := partitionInfo.ID
        partitionMS := s.cluster.partitionCache.findPartitionById(partitionId)
        if partitionMS == nil {
            log.Debug("ps heartbeat received a partition[%v] not existed.", partitionId)
            continue
        }

        //confVerMS :=
        //partitionMS.Partition.
            confVerHb := partitionInfo.Epoch.ConfVersion

    }

    replpbs := request.Replicas
    if replpbs == nil {
        resp.Header = makeRpcRespHeader(ErrSuc)
        return resp, nil
    }
    for _, replpb := range replpbs {
        partitionId := replpb.GetPartitionId()
        partition := rs.cluster.partitionCache.findPartitionById(partitionId)
        if partition == nil {
            log.Error("Cannot find partition belong to the replica. partitionId[%v]", partitionId)
            continue
        }

        partition.propertyLock.Lock() ////////////////////////////////

        // received heartbeat of an existed replica, do update the replica info
        replica := partition.replicaGroup.findReplicaById(replpb.GetId())
        if replica != nil {
            replica.update(replpb)
            continue
        }

        replicaNum := partition.replicaGroup.count()
        if replicaNum >= FIXED_REPLICA_NUM {
            log.Warn("The number[%v] of replicas for same one partition exceed fixed replica num", replicaNum)
            // TODO : delete replica
            continue
        }

        // TODO: modify space Status

        newReplica := NewReplica(replpb)
        if err := newReplica.persistent(rs.cluster.store); err != nil {
            continue
        }
        partition.replicaGroup.addReplica(replica, ps)
        ps.replicaCache.addReplica(replica)
    }

    resp.Header = makeRpcRespHeader(ErrSuc)
    return resp, nil
}

func makeRpcRespHeader(err error) metapb.ResponseHeader {
    code, ok := Err2CodeMap[err]
    if ok {
        return metapb.ResponseHeader{
            Code:    metapb.RespCode(code),
            Message: err.Error(),
        }
    } else {
        return metapb.ResponseHeader{
            Code:    ERRCODE_INTERNAL_ERROR,
            Message: ErrInternalError.Error(),
        }
    }
}
