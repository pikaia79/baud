package gm

import (
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/rpc"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"sync"
)

type RpcServer struct {
	config     *Config
	grpcServer *grpc.Server
	cluster    *Cluster
	wg         sync.WaitGroup
}

func NewRpcServer(config *Config, cluster *Cluster) *RpcServer {
	server := new(RpcServer)
	server.config = config
	server.cluster = cluster

	serverOption := &rpc.DefaultServerOption
	serverOption.ClusterID = config.ClusterCfg.ClusterID
	server.grpcServer = rpc.NewGrpcServer(serverOption)
	masterpb.RegisterGMRpcServer(server.grpcServer, server)
	reflection.Register(server.grpcServer)

	return server
}

func (s *RpcServer) Start() error {
	l, err := net.Listen("tcp", util.BuildAddr("0.0.0.0", s.config.ClusterCfg.RpcPort))
	if err != nil {
		log.Error("rpc server listen error[%v]", err)
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		if err := s.grpcServer.Serve(l); err != nil {
			log.Error("grpc server serve error[%v]", err)
		}
	}()

	log.Info("RPC server has started")
	return nil
}

func (s *RpcServer) Close() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
		s.grpcServer = nil
	}

	s.wg.Wait()

	log.Info("RPC server has closed")
}

func (s *RpcServer) GetDB(ctx context.Context, req *masterpb.GetDBRequest) (*masterpb.GetDBResponse, error) {
	resp := new(masterpb.GetDBResponse)

	db := s.cluster.DbCache.FindDbByName(req.DBName)
	if db == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrDbNotExists)
		return resp, nil
	}

	resp.Db = *db.DB
	resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)
	return resp, nil
}

func (s *RpcServer) GetSpace(ctx context.Context, req *masterpb.GetSpaceRequest) (*masterpb.GetSpaceResponse, error) {
	resp := new(masterpb.GetSpaceResponse)

	db := s.cluster.DbCache.FindDbById(req.ID)
	if db == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrDbNotExists)
		return resp, nil
	}

	space := db.SpaceCache.FindSpaceByName(req.SpaceName)
	if space == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrSpaceNotExists)
		return resp, nil
	}

	resp.Space = *space.Space
	resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)
	return resp, nil
}
