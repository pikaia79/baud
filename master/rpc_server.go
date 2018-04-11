package master

import (
	"proto/masterpb"
	"google.golang.org/grpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc/reflection"
	"net"
	"util/log"
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

func (rs *RpcServer) PsLogin(ctx context.Context, request *masterpb.PsLoginRequest) (*masterpb.PsLoginRequest, error) {
	return nil, nil
}

func (rs *RpcServer) CreateReplica(ctx context.Context, request *masterpb.CreateReplicaRequest) (*masterpb.CreateReplicaResponse, error) {
	return nil, nil
}

func (rs *RpcServer) PsHeartbeat(ctx context.Context, request *masterpb.PSHeartbeatRequest) (*masterpb.PSHeartbeatResponse, error) {
	return nil, nil
}
