package baudserver

import (
	"engine"
	"proto"
)

type Partition struct {
	store *engine.Store

	role proto.ReplicaRole
	status proto.PartitionStatus
}


