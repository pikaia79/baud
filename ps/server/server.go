package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"os"

	"github.com/pkg/errors"
	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
	"github.com/tiglabs/baud/ps/metric"
	"github.com/tiglabs/baud/util"
	"github.com/tiglabs/baud/util/build"
	"github.com/tiglabs/baud/util/config"
	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/netutil"
	"github.com/tiglabs/baud/util/rpc"
	"github.com/tiglabs/baud/util/timeutil"
	"github.com/tiglabs/baud/util/uuid"
	"github.com/tiglabs/raft"
)

const (
	registerTimeout = 10 * time.Second
)

// Server partition server
type Server struct {
	Config
	ip        string
	nodeID    metapb.NodeID
	ctx       context.Context
	ctxCancel context.CancelFunc

	nodeResolver *NodeResolver
	raftServer   *raft.RaftServer
	apiServer    *grpc.Server
	adminServer  *grpc.Server

	systemMetric *metric.SystemMetric

	connMgr         *rpc.ConnectionMgr
	masterClient    *rpc.Client
	masterHeartbeat *heartbeatWork

	meta       *serverMeta
	partitions sync.Map
}

// NewServer create server instance
func NewServer(conf *config.Config) *Server {
	serverConf := loadConfig(conf)
	if err := os.MkdirAll(serverConf.DataPath, os.ModePerm); err != nil {
		panic(err)
	}

	s := &Server{
		Config:       *serverConf,
		ip:           netutil.GetPrivateIP().String(),
		meta:         newServerMeta(serverConf.DataPath),
		nodeResolver: NewNodeResolver(),
		systemMetric: metric.NewSystemMetric(serverConf.DataPath, serverConf.DiskQuota),
	}
	s.nodeID = s.meta.MetaInfo.NodeID
	s.ctx, s.ctxCancel = context.WithCancel(context.Background())

	serverOpt := rpc.DefaultServerOption
	serverOpt.ClusterID = serverConf.ClusterID
	s.apiServer = rpc.NewGrpcServer(&serverOpt)
	s.adminServer = rpc.NewGrpcServer(&serverOpt)

	connMgrOpt := rpc.DefaultManagerOption
	s.connMgr = rpc.NewConnectionMgr(s.ctx, &connMgrOpt)

	clientOpt := rpc.DefaultClientOption
	clientOpt.Compression = true
	clientOpt.ClusterID = serverConf.ClusterID
	clientOpt.ConnectMgr = s.connMgr
	clientOpt.CreateFunc = func(cc *grpc.ClientConn) interface{} { return masterpb.NewMasterRpcClient(cc) }
	s.masterClient = rpc.NewClient(1, &clientOpt)

	s.masterHeartbeat = newHeartbeatWork(s)
	return s

}

// Start start server
func (s *Server) Start() error {
	registerResp, err := s.register()
	if err != nil {
		return err
	}
	if s.nodeID != registerResp.NodeID {
		s.nodeID = registerResp.NodeID
		if s.raftServer != nil {
			s.raftServer.Stop()
			s.raftServer = nil
		}
	}
	s.Config.PSConfig = registerResp.PSConfig
	if err := s.Config.validate(); err != nil {
		return err
	}

	// clear old partition
	if len(registerResp.Partitions) == 0 {
		s.destroyAll()
	} else {
		oldPartitions := s.meta.getInfo().Partitions
		for _, old := range oldPartitions {
			destroy := true
			for _, cur := range registerResp.Partitions {
				if cur.ID == old {
					destroy = false
					break
				}
			}

			if destroy {
				s.destroyPartition(old)
			}
		}
	}

	// create raft server
	if s.raftServer == nil {
		rc := raft.DefaultConfig()
		rc.NodeID = uint64(s.nodeID)
		rc.LeaseCheck = true
		rc.HeartbeatAddr = fmt.Sprintf(":%d", s.RaftHeartbeatPort)
		rc.ReplicateAddr = fmt.Sprintf(":%d", s.RaftReplicatePort)
		rc.Resolver = s.nodeResolver
		if s.RaftReplicaConcurrency > 0 {
			rc.MaxReplConcurrency = s.RaftReplicaConcurrency
		}
		if s.RaftSnapshotConcurrency > 0 {
			rc.MaxSnapConcurrency = s.RaftSnapshotConcurrency
		}
		if s.RaftHeartbeatInterval > 0 {
			rc.TickInterval = time.Millisecond * time.Duration(s.RaftHeartbeatInterval)
		}
		if s.RaftRetainLogs > 0 {
			rc.RetainLogs = s.RaftRetainLogs
		}

		rs, err := raft.NewRaftServer(rc)
		if err != nil {
			return fmt.Errorf("boot raft server failed, error: %v", err)
		}
		s.raftServer = rs
	}

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

func (s *Server) register() (*masterpb.PSRegisterResponse, error) {
	retryOpt := util.DefaultRetryOption
	retryOpt.MaxRetries = 5
	retryOpt.Context = s.ctx

	buildInfo := build.GetInfo()
	request := &masterpb.PSRegisterRequest{
		RequestHeader: metapb.RequestHeader{ReqId: uuid.FlakeUUID()},
		NodeID:        s.nodeID,
		Ip:            s.ip,
		RuntimeInfo: masterpb.RuntimeInfo{
			AppVersion: buildInfo.AppVersion,
			GoVersion:  buildInfo.GoVersion,
			Platform:   buildInfo.Platform,
			StartTime:  timeutil.FormatNow(),
		},
	}
	var response *masterpb.PSRegisterResponse

	err := util.RetryMaxAttempt(&retryOpt, func() error {
		masterClient, err := s.masterClient.GetGrpcClient(s.MasterServer)
		if err != nil {
			log.Error("get master register rpc client error: %v", err)
			return err
		}
		goCtx, cancel := context.WithTimeout(s.ctx, registerTimeout)
		resp, err := masterClient.(masterpb.MasterRpcClient).PSRegister(goCtx, request)
		cancel()

		if err != nil {
			log.Error("master register requeset[%s] failed error: %v", request.ReqId, err)
			return err
		}
		if resp.Code != metapb.RESP_CODE_OK {
			msg := fmt.Sprintf("master register requeset[%s] ack code not ok[%v], message is: %s", request.ReqId, resp.Code, resp.Message)
			log.Error(msg)
			return errors.New(msg)
		}

		response = resp
		return nil
	})

	return response, err
}
