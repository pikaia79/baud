package gm

import (
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
)

func getPartitionInfoByZone(zoneName string, partitionId metapb.PartitionID) (*masterpb.PartitionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	partitionInfoMeta, err := topoServer.GetPartitionInfoByZone(ctx, zoneName, partitionId)
	if err != nil {
		log.Error("topoServer GetPartitionInfoByZone error, err: [%v]", err)
		return nil, err
	}

	return partitionInfoMeta, nil
}

func getPartitionIdsByZone(zoneName string) ([]metapb.PartitionID, error) {
	if zoneName == topo.GlobalZone {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	partitionIds, err := topoServer.GetAllPartitionIdsByZone(ctx, zoneName)
	if err != nil {
		log.Error("topoServer GetAllPartitionIdsByZone error, err: [%v]", err)
		return nil, err
	}

	return partitionIds, nil
}

func pickLeaderReplica(partitionInfo *masterpb.PartitionInfo) *metapb.Replica {
	if partitionInfo == nil || !partitionInfo.IsLeader {
		return nil
	}

	return &partitionInfo.RaftStatus.Replica
}

func pickReplicaToDelete(partitionInfo *masterpb.PartitionInfo) *metapb.Replica {
	if partitionInfo == nil || partitionInfo.RaftStatus == nil {
		return nil
	}

	followers := partitionInfo.RaftStatus.Followers
	var replicaToDelete *metapb.Replica

	if !partitionInfo.IsLeader {
		replicaToDelete = &followers[0].Replica
		return replicaToDelete
	}

	leaderReplica := partitionInfo.RaftStatus.Replica
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
