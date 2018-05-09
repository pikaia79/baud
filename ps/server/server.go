package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/gogo/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/ps/metric"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/atomic"
	"github.com/tiglabs/baudengine/util/build"
	"github.com/tiglabs/baudengine/util/config"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/netutil"
	"github.com/tiglabs/baudengine/util/routine"
	"github.com/tiglabs/baudengine/util/rpc"
	"github.com/tiglabs/baudengine/util/timeutil"
	"github.com/tiglabs/baudengine/util/uuid"
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
	raftConfig   *raft.Config
	raftServer   *raft.RaftServer
	apiServer    *grpc.Server
	adminServer  *grpc.Server

	systemMetric *metric.SystemMetric

	connMgr         *rpc.ConnectionMgr
	masterLeader    string
	masterClient    *rpc.Client
	masterHeartbeat *heartbeatWork

	meta       *serverMeta
	partitions sync.Map

	stopping     atomic.AtomicBool
	adminEventCh chan proto.Message
}

// NewServer create server instance
func NewServer(conf *config.Config) *Server {
	serverConf := loadConfig(conf)
	log.InitFileLog(serverConf.LogDir, serverConf.LogModule, serverConf.LogLevel)

	s := &Server{
		Config:       *serverConf,
		ip:           netutil.GetPrivateIP().String(),
		meta:         newServerMeta(serverConf.DataPath),
		nodeResolver: NewNodeResolver(),
		systemMetric: metric.NewSystemMetric(serverConf.DataPath, serverConf.DiskQuota),
		adminEventCh: make(chan proto.Message, 64),
	}
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
	routine.RunWorkDaemon("ADMIN-EVENTHANDLER", s.adminEventHandler, s.ctx.Done())
	return s

}

// Start start server
func (s *Server) Start() error {
	return s.doStart(true)
}

func (s *Server) doStart(init bool) error {
	// load meta data
	if init {
		metaInfo := s.meta.getInfo()
		s.nodeID = metaInfo.NodeID
		if metaInfo.ClusterID != s.ClusterID {
			s.reset()
		}
	}

	// do register to master
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
		s.reset()
	} else {
		s.destroyExcludePartition(registerResp.Partitions)
	}
	s.meta.reset(&pspb.MetaInfo{ClusterID: s.ClusterID, NodeID: s.nodeID})

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
		s.raftConfig = rc
	}

	// create and recover partitions
	s.recoverPartitions(registerResp.Partitions)

	// Start server
	if init {
		if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.RPCPort)); err != nil {
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

		if ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.AdminPort)); err != nil {
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
	}

	// start heartbeat to master
	s.stopping.Set(false)
	s.masterHeartbeat.start()
	s.masterHeartbeat.trigger()

	return nil
}

// Stop stop server
func (s *Server) Close() error {
	s.stopping.Set(true)
	s.ctxCancel()

	if s.masterHeartbeat != nil {
		s.masterHeartbeat.stop()
	}
	if s.apiServer != nil {
		s.apiServer.GracefulStop()
	}
	if s.adminServer != nil {
		s.adminServer.GracefulStop()
	}

	routine.Stop()
	s.closeAllRange()

	if s.raftServer != nil {
		s.raftServer.Stop()
	}
	if s.masterClient != nil {
		s.masterClient.Close()
	}
	if s.connMgr != nil {
		s.connMgr.Close()
	}

	return nil
}

func (s *Server) closeAllRange() {
	s.partitions.Range(func(key, value interface{}) bool {
		p := value.(*partition)
		p.Close()
		s.partitions.Delete(p.meta.ID)

		for _, r := range p.meta.Replicas {
			s.nodeResolver.deleteNode(r.NodeID)
		}
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
		masterAddr := s.MasterServer
		if s.masterLeader != "" {
			masterAddr = s.masterLeader
		}
		masterClient, err := s.masterClient.GetGrpcClient(masterAddr)
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
			if resp.Error.NoLeader != nil {
				s.masterLeader = ""
			} else if resp.Error.NotLeader != nil {
				s.masterLeader = resp.Error.NotLeader.LeaderAddr
			}
			return errors.New(msg)
		}

		response = resp
		return nil
	})

	return response, err
}

func (s *Server) recoverPartitions(partitions []metapb.Partition) {
	// sort by partition id
	sort.Sort(PartitionByIdSlice(partitions))
	wg := new(sync.WaitGroup)
	wg.Add(len(partitions))

	// parallel execution recovery
	for i := 0; i < len(partitions); i++ {
		p := partitions[i]
		routine.RunWorkAsync("RECOVER-PARTITION", func() {
			defer wg.Done()

			s.doPartitionCreate(p)
		}, routine.LogPanic(false))
	}

	wg.Wait()
}

func (s *Server) restart() {
	// do close
	s.stopping.Set(true)
	s.masterHeartbeat.stop()
	s.masterClient.Close()

	// clear admin event channel
	endFlag := false
	for {
		select {
		case <-s.adminEventCh:
		default:
			endFlag = true
		}

		if endFlag {
			break
		}
	}

	// do start
	s.closeAllRange()
	if err := s.doStart(false); err != nil {
		panic(fmt.Errorf("restart error: %v", err))
	}
}
