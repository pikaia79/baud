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
	"github.com/tiglabs/baudengine/util/rpc"
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
	masterpb.RegisterMasterRpcServer(server.grpcServer, server)
	reflection.Register(server.grpcServer)

	return server
}

func (s *RpcServer) Start() error {
	l, err := net.Listen("tcp", util.BuildAddr("0.0.0.0", s.config.ClusterCfg.CurNode.RpcPort))
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

func (s *RpcServer) GetRoute(ctx context.Context,
	req *masterpb.GetRouteRequest) (*masterpb.GetRouteResponse, error) {
	resp := new(masterpb.GetRouteResponse)

	db := s.cluster.DbCache.FindDbById(req.DB)
	if db == nil {
		resp.ResponseHeader = *makeRpcRespHeader(ErrDbNotExists)
		return resp, nil
	}

	space := db.SpaceCache.FindSpaceById(req.Space)
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
				ps := s.cluster.PsCache.FindServerById(replica.NodeID)
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

func (s *RpcServer) PSRegister(ctx context.Context,
	req *masterpb.PSRegisterRequest) (*masterpb.PSRegisterResponse, error) {
	resp := new(masterpb.PSRegisterResponse)

    if err, msLeader := s.validateLeader(); err != nil {
        resp.ResponseHeader = *makeRpcRespHeaderWithError(err, msLeader)
        return resp, nil
    }

	nodeId := req.NodeID

	if nodeId == 0 {
		// this is a new ps unregistered never, distribute new psid to it
		ps, err := NewPartitionServer(req.Ip, &s.config.PsCfg)
		if err != nil {
			resp.ResponseHeader = *makeRpcRespHeader(err)
			return resp, nil
		}
		ps.persistent(s.cluster.store)

		ps.status = PS_REGISTERED
		s.cluster.PsCache.AddServer(ps)

		resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)
		resp.NodeID = ps.ID
		packPsRegRespWithCfg(resp, &s.config.PsCfg)
        log.Debug("new register response [%v]", resp)
		return resp, nil
	}

	// use nodeid reserved by ps to recognize same one ps
	ps := s.cluster.PsCache.FindServerById(nodeId)
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
	packPsRegRespWithCfg(resp, &s.config.PsCfg)
	resp.Partitions = *ps.partitionCache.GetAllMetaPartitions()
    log.Debug("old nodeid[%v] register response [%v]", nodeId, resp)

	return resp, nil
}

func (s *RpcServer) PSHeartbeat(ctx context.Context,
	req *masterpb.PSHeartbeatRequest) (*masterpb.PSHeartbeatResponse, error) {
	log.Debug("Received PS Heartbeat req:[%v]", req)
	resp := new(masterpb.PSHeartbeatResponse)
	resp.ResponseHeader = *makeRpcRespHeader(ErrSuc)

    if err, msLeader := s.validateLeader(); err != nil {
        resp.ResponseHeader = *makeRpcRespHeaderWithError(err, msLeader)
        return resp, nil
    }

	// process ps
	psId := req.NodeID
	ps := s.cluster.PsCache.FindServerById(psId)
	if ps == nil {
		log.Error("ps heartbeat received invalid ps. id[%v]", psId)
		resp.ResponseHeader = *makeRpcRespHeader(ErrPSNotExists)
		return resp, nil
	}
	ps.updateHb()

	partitionInfos := req.Partitions
	if partitionInfos == nil {
		// TODO:this is empty ps, check ps status to destroy ps or create new partition to it
		return resp, nil
	}

	// process partition
	for _, partitionInfo := range partitionInfos {
		if err := s.validatePartitionInfo(&partitionInfo); err != nil {
			resp.ResponseHeader = *makeRpcRespHeader(err)
			return resp, nil
		}

		partitionId := partitionInfo.ID
		partitionMS := s.cluster.PartitionCache.FindPartitionById(partitionId)
		if partitionMS == nil {
			log.Info("ps heartbeat received a partition[%v], that not existed in cluster.", partitionId)
			// force to delete
			if replicaToDelete := pickReplicaToDelete(&partitionInfo); replicaToDelete != nil {
				GetPMSingle(nil).PushEvent(NewForcePartitionDeleteEvent(partitionId, ps.getRpcAddr(), replicaToDelete))
			}
			continue
		}

		confVerMS := partitionMS.Epoch.ConfVersion
		confVerHb := partitionInfo.Epoch.ConfVersion
		log.Info("partition id[%v], confVerHb[%v], confVerMS[%v]", partitionId, confVerHb, confVerMS)
		if confVerHb > confVerMS {
			if !partitionInfo.IsLeader {
				return resp, nil
			}

			leaderFollowerHb, err := pickLeaderFollower(&partitionInfo)
			if err != nil {
				log.Error("Not found leader replica id from info. err[%v]", err)
				resp.ResponseHeader = *makeRpcRespHeader(err)
				return resp, nil
			}
			// force to update by leader
			if condOk, err := partitionMS.UpdateReplicaGroupUnderGreatOrZeroVer(s.cluster.store, &partitionInfo,
						leaderFollowerHb); !condOk || err != nil {
                log.Debug("Fail to update partition[%v] info. waiting next heartbeat. condOk[%v], err[%v]",
                    partitionInfo.ID, condOk, err)
				return resp, nil
			}

			// add or delete replicas
			replicaCount := partitionMS.countReplicas()
			if replicaCount > FIXED_REPLICA_NUM {
				// the count of heartbeat replicas may be great then 4 when making snapshot.
				// TODO: check partition status is not transfering replica now, then to delete

				log.Info("Too many replicas added. cur count:[%v]", replicaCount)
				if !partitionMS.takeChangeMemberTask() {
					return resp, nil
				}

				if replicaToDelete := pickReplicaToDelete(&partitionInfo); replicaToDelete != nil {
					GetPMSingle(nil).PushEvent(NewPartitionDeleteEvent(partitionInfo.ID, partitionMS.pickLeaderNodeId(),
						replicaToDelete))
				}

			} else if replicaCount < FIXED_REPLICA_NUM {

				log.Info("Too little replicas added. cur count:[%v]", replicaCount)
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

			if replicaToDelete := pickReplicaToDelete(&partitionInfo); replicaToDelete != nil {
				GetPMSingle(nil).PushEvent(NewPartitionDeleteEvent(partitionInfo.ID, partitionMS.pickLeaderNodeId(),
					replicaToDelete))
			}

		} else if confVerHb == confVerMS {
			if !partitionInfo.IsLeader {
				return resp, nil
			}

			leaderFollowerHb, err := pickLeaderFollower(&partitionInfo)
			if err != nil {
				log.Error("Not found leader replica id from info. err[%v]", err)
				resp.ResponseHeader = *makeRpcRespHeader(err)
				return resp, nil
			}

			if confVerMS == 0 {
				if condOk, err := partitionMS.UpdateReplicaGroupUnderGreatOrZeroVer(s.cluster.store, &partitionInfo,
					leaderFollowerHb);  !condOk || err != nil {
					log.Debug("Fail to update partition[%v] info. waiting next heartbeat. condOk[%v], err[%v]",
					        partitionInfo.ID, condOk, err)
					return resp, nil
				}

			} else {  // if confVerMS != 0
				condOk, updateOk := partitionMS.UpdateLeaderUnderSameVer(&partitionInfo, leaderFollowerHb)
				if !condOk {
					log.Debug("ConfVersion is expired. waiting next heartbeat")
					return resp, nil
				}
				if !updateOk {
					log.Info("To delete replicas of partition[%v] that had same conf version",
						"but member[%v] is unmatched for cluster.", partitionInfo.ID, leaderFollowerHb)

					if replicaToDelete := pickReplicaToDelete(&partitionInfo); replicaToDelete != nil {
						log.Info("try to delete replica[%v]", replicaToDelete)
						GetPMSingle(nil).PushEvent(NewPartitionDeleteEvent(partitionInfo.ID, leaderFollowerHb.NodeID,
							replicaToDelete))
					}
                    return resp, nil
				}
			}

			// TODO: add or delete replicas
		}
	}

	return resp, nil
}

func (s *RpcServer) validateLeader() (error, interface{}) {
    leaderInfo :=  s.cluster.store.GetLeaderSync()
    if leaderInfo == nil {
        return ErrNoMSLeader, nil
    }

	if !leaderInfo.becomeLeader {
		if leaderInfo.newLeaderId == 0 {
			return ErrNoMSLeader, nil
		} else {
			return ErrNotMSLeader, &metapb.NotLeader{
				Leader:     metapb.NodeID(leaderInfo.newLeaderId),
				LeaderAddr: leaderInfo.newLeaderAddr,
			}
		}
	}

    return nil, leaderInfo.newLeaderId
}

func (s *RpcServer) validatePartitionInfo(info *masterpb.PartitionInfo) error {
	if info == nil {
		return ErrInternalError
	}

	//if info.IsLeader && info.RaftStatus == nil ||
	//		info.RaftStatus.Followers == nil || len(info.RaftStatus.Followers) == 0) {
	//	log.Error("!!!Never happened. Cannot report empty replicas when info is leader. info id[%v]", info.ID)
	//	return ErrRpcEmptyFollowers
	//}
    //
	//if info.IsLeader {
	//	followers := info.RaftStatus.Followers
	//	leaderReplica := info.RaftStatus.Replica
    //
	//	var leaderFound bool
	//	for _, follower := range followers {
	//		if follower.ID == leaderReplica.ID {
	//			leaderFound = true
	//			break
	//		}
	//	}
	//	if !leaderFound {
	//		return ErrRpcNoFollowerLeader
	//	}
	//}
	return nil
}

func pickLeaderFollower(info *masterpb.PartitionInfo) (*metapb.Replica, error) {
	if !info.IsLeader {
		return nil, ErrNotMSLeader
	}

	return &info.RaftStatus.Replica, nil
}

func pickReplicaToDelete(info *masterpb.PartitionInfo) (*metapb.Replica) {
	if info == nil || info.RaftStatus == nil {
		return nil
	}

	followers := info.RaftStatus.Followers
	var replicaToDelete *metapb.Replica

	if !info.IsLeader {
		replicaToDelete = &followers[0].Replica
		return replicaToDelete
	}

	leaderReplica := info.RaftStatus.Replica
	for _, follower := range followers {
		if follower.ID == leaderReplica.ID {
			continue
		}

        replicaToDelete = &follower.Replica
		break
	}
	if replicaToDelete != nil {
		return replicaToDelete
	}

	return &leaderReplica
}

func packPsRegRespWithCfg(resp *masterpb.PSRegisterResponse, psCfg *PsConfig) {
	resp.RPCPort = int(psCfg.RpcPort)
	resp.AdminPort = int(psCfg.AdminPort)
	resp.HeartbeatInterval = int(psCfg.HeartbeatInterval)
	resp.RaftHeartbeatInterval = int(psCfg.RaftHeartbeatInterval)
	resp.RaftHeartbeatPort = int(psCfg.RaftHeartbeatPort)
	resp.RaftReplicatePort = int(psCfg.RaftReplicatePort)
	resp.RaftRetainLogs = psCfg.RaftRetainLogs
	resp.RaftReplicaConcurrency = int(psCfg.RaftReplicaConcurrency)
	resp.RaftSnapshotConcurrency = int(psCfg.RaftSnapshotConcurrency)
}
