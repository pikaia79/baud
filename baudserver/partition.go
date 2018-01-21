package baudserver

import (
	"engine"
	"proto"
)

type Partition struct {
	store *store.Store

	role proto.ReplicaRole
	status proto.PartitionStatus
}


