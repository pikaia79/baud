package master

import (
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
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
	l, err := net.Listen("tcp", util.BuildAddr("0.0.0.0", int(s.config.ClusterCfg.CurNode.RpcPort)))
	if err != nil {
		log.Error("rpc server listen error[%v]", err)
		return err
	}

	go func() {
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
	}
	log.Info("RPC server has closed")
}

func (s *RpcServer) GetRoute(ctx context.Context,
	req *masterpb.GetRouteRequest) (*masterpb.GetRouteResponse, error) {
	resp := new(masterpb.GetRouteResponse)

	db := s.cluster.dbCache.findDbById(req.DB)
	if db == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrDbNotExists)
		return resp, nil
	}

	space := db.spaceCache.findSpaceById(req.Space)
	if space == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrSpaceNotExists)
		return resp, nil
	}

	partitions := space.searchTree.multipleSearch(req.Slot, 10)
	if partitions == nil || len(partitions) == 0 {
		resp.ResponseHeader = *makeRpcRespHeader(ErrRouteNotFound)
		return resp, nil
	}

	resp.Routes = make([]masterpb.Route, 0, len(partitions))
	for _, partition := range partitions {
		route := masterpb.Route{
			Partition: *partition.Partition,
			Leader:    partition.pickLeaderNodeId(),
		}

		replicas := partition.Replicas
		if replicas != nil || len(replicas) != 0 {
			nodes := make([]*metapb.Node, 0, len(replicas))
			for _, replica := range replicas {
				ps := s.cluster.psCache.findServerById(replica.NodeID)
				if ps != nil {
					nodes = append(nodes, ps.Node)
				}
			}
			route.Nodes = nodes
		}

		resp.Routes = append(resp.Routes, route)
	}
	resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)

	return resp, nil
}

func (s *RpcServer) GetDB(ctx context.Context, req *masterpb.GetDBRequest) (*masterpb.GetDBResponse, error) {
	resp := new(masterpb.GetDBResponse)

	db := s.cluster.dbCache.findDbByName(req.DBName)
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

	db := s.cluster.dbCache.findDbById(req.ID)
	if db == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrDbNotExists)
		return resp, nil
	}

	space := db.spaceCache.findSpaceByName(req.SpaceName)
	if space == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrSpaceNotExists)
		return resp, nil
	}

	resp.Space = *space.Space
	resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)
	return resp, nil
}

func (s *RpcServer) PSRegister(ctx context.Context,
	req *masterpb.PSRegisterRequest) (*masterpb.PSRegisterResponse, error) {
	resp := new(masterpb.PSRegisterResponse)

	nodeId := req.NodeID

	if nodeId == 0 {
		// this is a new ps unregistered never, distribute new psid to it
		ps, err := NewPartitionServer(req.Ip)
		if err != nil {
			resp.ResponseHeader = *makeRpcRespHeader(err)
			return resp, nil
		}
		ps.persistent(s.cluster.store)

		ps.status = PS_REGISTERED
		s.cluster.psCache.addServer(ps)

		resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)
		resp.NodeID = ps.ID
		return resp, nil
	}

	// use nodeid reserved by ps to recognize same one ps
	ps := s.cluster.psCache.findServerById(nodeId)
	if ps == nil {
		// illegal ps will register
		log.Warn("Can not find nodeid[%v] in master.", nodeId)
		resp.ResponseHeader = *makeRpcRespHeader(ErrPSNotExists)
		return resp, nil
	}

	// old ps rebooted
	ps.changeStatus(PS_REGISTERED)

	resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)
	resp.NodeID = ps.ID
	resp.Partitions = *ps.partitionCache.getAllMetaPartitions()

	return resp, nil
}

func (s *RpcServer) PSHeartbeat(ctx context.Context,
	req *masterpb.PSHeartbeatRequest) (*masterpb.PSHeartbeatResponse, error) {
	resp := new(masterpb.PSHeartbeatResponse)
	resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)

	// process ps
	psId := req.NodeID
	ps := s.cluster.psCache.findServerById(psId)
	if ps == nil {
		log.Error("ps heartbeat received invalid psid[%v]", psId)
		resp.ResponseHeader = *makeRpcRespHeader(ErrPSNotExists)
		return resp, nil
	}
	ps.updateHb()

	partitionInfos := req.Partitions
	if partitionInfos == nil {
		// TODO:check ps status to destroy ps or create new partition to it
		return resp, nil
	}

	// process partition
	for _, partitionInfo := range partitionInfos {
		partitionId := partitionInfo.ID
		partitionMS := s.cluster.partitionCache.findPartitionById(partitionId)
		if partitionMS == nil {
			log.Debug("ps heartbeat received a partition[%v] not existed.", partitionId)

			// force to delete
			replicaToDelete, err := pickReplicaToDelete(&partitionInfo, partitionMS.pickLeaderReplica().NodeID)
			if err != nil {
				resp.ResponseHeader = *makeRpcRespHeader(err)
				return resp, nil
			}
			GetPMSingle(nil).PushEvent(NewForcePartitionDeleteEvent(partitionId, ps.getRpcAddr(), replicaToDelete))

			continue
		}

		confVerMS := partitionMS.Epoch.ConfVersion
		confVerHb := partitionInfo.Epoch.ConfVersion
		if confVerHb > confVerMS {
			if !partitionInfo.IsLeader {
				return resp, nil
			}

			// force to update by leader
			if err := partitionMS.updateInfo(s.cluster.store, &partitionInfo, req.NodeID); err != nil {
				log.Error("fail to update partition[%v] info in ps heartbeat. err[%v]", partitionInfo.ID, err)
				resp.ResponseHeader = *makeRpcRespHeader(ErrInternalError)
				return resp, nil
			}

			// add or delete replicas
			replicaCount := partitionMS.countReplicas()
			if replicaCount > FIXED_REPLICA_NUM {
				// the count of heartbeat replicas may be great then 4 when making snapshot.
				// TODO: check partition status is not transfering replica now, then to delete

				log.Info("To many replicas added. count:[%v]", replicaCount)
				if !partitionMS.takeChangeMemberTask() {
					return resp, nil
				}

				replicaToDelete, err := pickReplicaToDelete(&partitionInfo, partitionMS.pickLeaderNodeId())
				if err != nil {
					resp.ResponseHeader = *makeRpcRespHeader(err)
					return resp, nil
				}

				GetPMSingle(nil).PushEvent(NewPartitionDeleteEvent(partitionInfo.ID, partitionMS.pickLeaderNodeId(),
					replicaToDelete))

			} else if replicaCount < FIXED_REPLICA_NUM {

				log.Info("To little replicas added. count:[%v]", replicaCount)
				if !partitionMS.takeChangeMemberTask() {
					return resp, nil
				}

				GetPMSingle(nil).PushEvent(NewPartitionCreateEvent(partitionMS))

			} else {
				log.Info("Normal replica count in heartbeat")
			}

		} else if confVerHb < confVerMS {
			// force delete all replicas and leader
			if !partitionMS.takeChangeMemberTask() {
				return resp, nil
			}

			replicaToDelete, err := pickReplicaToDelete(&partitionInfo, partitionMS.pickLeaderNodeId())
			if err != nil {
				resp.ResponseHeader = *makeRpcRespHeader(err)
				return resp, nil
			}
			GetPMSingle(nil).PushEvent(NewPartitionDeleteEvent(partitionInfo.ID, partitionMS.pickLeaderNodeId(),
				replicaToDelete))

		} else {
			// timeout delete in same conf version when have been not elected leader in ps at a long time

		}
	}

	return resp, nil
}

func pickReplicaToDelete(info *masterpb.PartitionInfo, leaderNodeId metapb.NodeID) (*metapb.Replica, error) {
	followers := info.RaftStatus.Followers
	if followers == nil || len(followers) == 0 {
		log.Error("!!!Never happened. Cannot report empty replicas in ps heartbeat. info:[%v]", info)
		return nil, ErrRpcEmptyFollowers
	}

	var replicaToDelete *metapb.Replica

	for {
		if !info.IsLeader {
			replicaToDelete = &metapb.Replica{ID: followers[0].ID, NodeID: followers[0].NodeID}
			break
		}

		// firstly pick followers, finally leader
		if len(followers) > 1 {
			for _, follower := range followers {
				if follower.NodeID != leaderNodeId {
					replicaToDelete = &metapb.Replica{ID: follower.ID, NodeID: follower.NodeID}
					break
				}
			}

			log.Error("cannot find leader in followers")
			return nil, ErrRpcInvalidFollowers

		} else {
			replicaToDelete = &metapb.Replica{ID: followers[0].ID, NodeID: followers[0].NodeID}
			break
		}

	}

	return replicaToDelete, nil
}

func makeRpcRespHeader(err error) *metapb.ResponseHeader {
	code, ok := Err2CodeMap[err]
	if ok {
		return &metapb.ResponseHeader{
			Code:    metapb.RespCode(code),
			Message: err.Error(),
		}
	} else {
		return &metapb.ResponseHeader{
			Code:    ERRCODE_INTERNAL_ERROR,
			Message: ErrInternalError.Error(),
		}
	}
}
