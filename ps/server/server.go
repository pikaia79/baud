package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"context"

	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
	"github.com/tiglabs/baud/ps/metric"
	"github.com/tiglabs/baud/util/config"
	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/netutil"
	"github.com/tiglabs/baud/util/rpc"
	"github.com/tiglabs/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server partition server
type Server struct {
	config       Config
	nodeID       metapb.NodeID
	nodeResolver *NodeResolver
	partitions   *sync.Map
	context      context.Context
	quit         chan struct{}

	apiServer   *grpc.Server
	adminServer *grpc.Server
	raftServer  *raft.RaftServer

	masterClient *rpc.Client

	systemMetric *metric.SystemMetric
}

// NewServer create server instance
func NewServer(conf *config.Config) (*Server, error) {
	serverConf, err := LoadConfig(conf)
	if err != nil {
		return nil, err
	}

	ip := netutil.GetPrivateIP()
	// setting raft config
	resolver := NewNodeResolver()
	rc := raft.DefaultConfig()
	rc.RetainLogs = serverConf.RaftRetainLogs
	rc.TickInterval = time.Millisecond * time.Duration(serverConf.RaftHeartbeatInterval)
	rc.HeartbeatAddr = fmt.Sprintf(":%d", serverConf.RaftHeartbeatPort)
	rc.ReplicateAddr = fmt.Sprintf(":%d", serverConf.RaftReplicatePort)
	rc.Resolver = resolver
	rc.MaxReplConcurrency = serverConf.RaftReplicaConcurrency
	rc.MaxSnapConcurrency = serverConf.RaftSnapshotConcurrency
	rc.NodeID = 0
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		return nil, fmt.Errorf("boot raft server failed, error: %v", err)
	}

	s := &Server{
		config:       *serverConf,
		nodeID:       0,
		nodeResolver: resolver,
		partitions:   &sync.Map{},
		quit:         make(chan struct{}),
		raftServer:   rs,
		context:      context.WithCancel(context.Background()),
	}

	serverOpt := rpc.DefaultServerOption
	serverOpt.ClusterID = serverConf.ClusterID
	s.apiServer = rpc.NewGrpcServer(&serverOpt)
	s.adminServer = rpc.NewGrpcServer(&serverOpt)

	connMgrOpt := rpc.DefaultManagerOption
	connMgr := rpc.NewConnectionMgr(s.context, &connMgrOpt)
	clientOpt := rpc.DefaultClientOption
	clientOpt.ClusterID = serverConf.ClusterID
	clientOpt.ConnectMgr = connMgr
	clientOpt.CreateFunc = func(cc *grpc.ClientConn) interface{} { return masterpb.NewMasterRpcClient(cc) }
	s.masterClient = rpc.NewClient(1, &clientOpt)

	return s, nil
}

// Start start server
func (s *Server) Start() error {
	if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.RPCPort)); err != nil {
		return fmt.Errorf("Server failed to listen api port: %v", err)
	} else {
		pspb.RegisterApiGrpcServer(s.apiServer, s)
		reflection.Register(s.apiServer)
		go func() {
			if err = s.apiServer.Serve(ln); err != nil {
				log.Fatal("Server failed to start api grpc: %v", err)
			}
		}()
	}

	if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Config.AdminPort)); err != nil {
		return fmt.Errorf("Server failed to listen admin port: %v", err)
	} else {
		pspb.RegisterAdminGrpcServer(s.adminServer, s)
		reflection.Register(s.adminServer)
		go func() {
			if err = s.adminServer.Serve(ln); err != nil {
				log.Fatal("Server failed to start admin grpc: %v", err)
			}
		}()
	}

	return nil
}

// Stop stop server
func (s *Server) Stop() {
	close(s.quit)
	if s.apiServer != nil {
		s.apiServer.GracefulStop()
	}
	if s.adminServer != nil {
		s.adminServer.GracefulStop()
	}
	s.closeAllRange()
}

func (s *Server) closeAllRange() {
	s.partitions.Range(func(key, value interface{}) bool {
		value.(*partition).Close()
		return true
	})
}

func (s *Server) resgitry() {

}
