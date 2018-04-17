package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
	"github.com/tiglabs/baud/ps/rpc"
	"github.com/tiglabs/baud/util/config"
	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/netutil"
	"github.com/tiglabs/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server partition server
type Server struct {
	Config
	raftConf     raft.Config
	node         metapb.Node
	nodeResolver *NodeResolver
	partitions   *sync.Map
	quit         chan struct{}

	apiServer   *grpc.Server
	adminServer *grpc.Server
	raftServer  *raft.RaftServer

	masterClient *masterpb.MasterRpcClient
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
	rc.HeartbeatAddr = serverConf.RaftHeartbeatAddr
	rc.ReplicateAddr = serverConf.RaftReplicaAddr
	rc.Resolver = resolver
	rc.MaxReplConcurrency = serverConf.RaftReplicaConcurrency
	rc.MaxSnapConcurrency = serverConf.RaftSnapshotConcurrency
	rc.NodeID = uint64(serverConf.NodeID)
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		return nil, fmt.Errorf("boot raft server failed, error: %v", err)
	}

	// self info
	node := metapb.Node{
		ID:   serverConf.NodeID,
		Ip:   ip.String(),
		Port: serverConf.RPCPort,
		RaftAddrs: metapb.RaftAddrs{
			HeartbeatAddr: serverConf.RaftHeartbeatAddr,
			ReplicateAddr: serverConf.RaftReplicaAddr,
		},
	}

	s := &Server{
		Config:       *serverConf,
		raftConf:     *rc,
		node:         node,
		nodeResolver: resolver,
		partitions:   &sync.Map{},
		quit:         make(chan struct{}),
		raftServer:   rs,
	}
	s.apiServer = rpc.CreateGrpcServer(rpc.DefaultOption())
	s.adminServer = rpc.CreateGrpcServer(rpc.DefaultOption())

	return s, nil
}

// Start start server
func (s *Server) Start() error {
	if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Config.RPCPort)); err != nil {
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
