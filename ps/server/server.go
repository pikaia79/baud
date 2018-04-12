package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/tiglabs/baud/proto/pspb"
	"github.com/tiglabs/baud/ps/rpc"
	"github.com/tiglabs/baud/util/config"
	"github.com/tiglabs/baud/util/log"
	netSvr "github.com/tiglabs/baud/util/server"
	"github.com/tiglabs/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Server partition server
type Server struct {
	conf         *Config
	raftConf     *raft.Config
	node         *pspb.Node
	nodeResolver *NodeResolver
	partitions   *sync.Map
	quit         chan struct{}

	httpServer *netSvr.Server
	grpcServer *grpc.Server
	raftServer *raft.RaftServer
}

// NewServer create server instance
func NewServer(conf *config.Config) (*Server, error) {
	serverConf, err := LoadConfig(conf)
	if err != nil {
		return nil, err
	}

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
	rc.NodeID = serverConf.NodeID
	rs, err := raft.NewRaftServer(rc)
	if err != nil {
		return nil, fmt.Errorf("boot raft server failed, error: %v", err)
	}

	// self info
	node := &pspb.Node{
		ID:      serverConf.NodeID,
		Address: fmt.Sprintf(":%d", serverConf.RPCPort),
		State:   pspb.STATE_INITIAL,
		RaftAddrs: &pspb.RaftAddrs{
			HeartbeatAddr: serverConf.RaftHeartbeatAddr,
			ReplicateAddr: serverConf.RaftReplicaAddr,
		},
		Version: "",
	}

	s := &Server{
		conf:         serverConf,
		raftConf:     rc,
		node:         node,
		nodeResolver: resolver,
		partitions:   &sync.Map{},
		quit:         make(chan struct{}),
		raftServer:   rs,
	}
	s.grpcServer = rpc.CreateGrpcServer(rpc.DefaultOption())

	return s, nil
}

// Start start server
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.conf.RPCPort))
	if err != nil {
		return fmt.Errorf("Server failed to listen: %v", err)
	}
	pspb.RegisterInternalServer(s.grpcServer, s)
	reflection.Register(s.grpcServer)
	go func() {
		if err = s.grpcServer.Serve(ln); err != nil {
			log.Fatal("Server failed to start grpc: %v", err)
		}
	}()

	return nil
}

// Stop stop server
func (s *Server) Stop() {
	close(s.quit)
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
	s.closeAllRange()
}

func (s *Server) closeAllRange() {
	s.partitions.Range(func(key, value interface{}) bool {
		value.(*partition).Close()
		return true
	})
}
