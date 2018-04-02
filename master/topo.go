package master

import (
	"github.com/google/btree"
	"time"
)

type TopoInfo struct {
	Spaces				map[string]SpaceInfo
	Partitions			map[uint32]PartitionInfo
	PSInfos				map[uint32]ServerInfo
}

type SpaceInfo struct {
	SpaceName			string
	PartIds 			[]uint32
	sortedPartitions	btree.BTree
}

type PartitionInfo struct {
	PartId       		uint32
	SpaceName			string

	startSlotId			uint32
	endSlotId 			uint32
	replicas 			map[uint32]ReplicaInfo

	leaderReplicaId		uint32
}

type ReplicaInfo struct {
	ReplicaId 			uint32
	isLeader			bool
	LastHbTime 			time.Time // only belong to leader
	//State
	PartId 				uint32
	PSId 				uint32
}

type PSState 	uint32

const (
	PSState_Invalid 	PSState = iota
	// 工作状态，可以提供服务
	PSState_Login
	// 此状态下节点不分配新的range，不迁移range
	PSState_Offline
	// 此状态下节点不分配新的range，开始逐步迁移range
	PSState_N_Tombstone
	// 此状态下节点的range已经全部迁移，必须手动login
	PSState_Logout
)

type ServerInfo struct {
	PSId			uint32
	Addr			string
	PartId 			uint32
	ReplicaId 		uint32
	State 			PSState
	LastHbTime  	time.Time
}
