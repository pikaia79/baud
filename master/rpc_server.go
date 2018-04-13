package master

import (
	"proto/masterpb"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc/reflection"
	"net"
	"util/log"
	//"proto/metapb"
	"proto/metapb"
	"util"
)

type RpcServer struct {
	config 		*Config
	grpcServer 	*grpc.Server

	cluster 	*Cluster
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

func (rs *RpcServer) Start() error {
	l, err := net.Listen("tcp", rs.config.rpcServerAddr)
	if err != nil {
		log.Error("rpc server listen error[%v]", err)
		return err
	}

	go func() {
		if err := rs.grpcServer.Serve(l); err != nil {
			log.Error("grpc server serve error[%v]", err)
		}
	}()

	return nil
}

func (rs *RpcServer) Close() {
	if rs.grpcServer != nil {
		rs.grpcServer.GracefulStop()
	}
}

func (rs *RpcServer) GetRoute(ctx context.Context, request *masterpb.GetRouteRequest) (*masterpb.GetRouteResponse, error) {
	return nil, nil
}

func (rs *RpcServer) PsLogin(ctx context.Context, request *masterpb.PsLoginRequest) (*masterpb.PsLoginResponse, error) {
	resp := new(masterpb.PsLoginResponse)

	server := request.GetServer()
	resource := request.GetServerResource()
	if server == nil || resource == nil {
		resp.Header = makeRpcRespHeader(ErrGrpcParamError)
		return resp, nil
	}

	// use immutable ip and port to recognize same one ps
	addr := util.BuildAddr(server.GetIp(), server.GetPort())
	ps := rs.cluster.psCache.findServerByAddr(addr)
	if ps != nil {
		// old ps rebooted
		ps.changeStatus(metapb.PSStatus_PS_Login)
	} else {
		// this is a new ps unregistered never, distribute new psid to it
		psId, err := GetIdGeneratorInstance(nil).GenID()
		if err != nil {
			log.Error("fail to generate ps id. err[%v]", err)
			resp.Header = makeRpcRespHeader(ErrGenIdFailed)
			return resp, nil
		}
		server.Id = psId

		ps := NewPartitionServer(server, resource)
		ps.changeStatus(metapb.PSStatus_PS_Login)
		rs.cluster.psCache.addServer(ps)
	}

	resp.Header = makeRpcRespHeader(ErrSuc)
	resp.Server = ps.PartitionServer
	return resp, nil
}

func (rs *RpcServer) PsHeartbeat(ctx context.Context, request *masterpb.PSHeartbeatRequest) (*masterpb.PSHeartbeatResponse, error) {
	resp := new(masterpb.PSHeartbeatResponse)

	psId := request.GetPsId()
	ps := rs.cluster.psCache.findServerById(psId)
	if ps == nil {
		log.Error("psid[%v] is not be found", psId)
		resp.Header = makeRpcRespHeader(ErrPSNotExists)
		return resp, nil
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

		partition.propertyLock.Lock()////////////////////////////////

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

		// TODO: modify space status

		newReplica := NewReplica(replpb)
		if err := newReplica.persistent(rs.cluster.store); err != nil {
			continue
		}
		partition.replicaGroup.addReplica(replica, ps)
		ps.replicaCache.addReplica(replica)
	}

	return nil, nil
}

func makeRpcRespHeader(err error) *metapb.ResponseHeader {
	code, ok := Err2CodeMap[err.Error()]
	if ok {
		return &metapb.ResponseHeader{
			Code: code,
			Msg:  err.Error(),
		}
	} else {
		return &metapb.ResponseHeader{
			Code:		ERRCODE_INTERNAL_ERROR,
			Msg:		ErrInternalError.Error(),
		}
	}
}
