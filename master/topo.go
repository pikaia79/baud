package master

import (
	. "github.com/tiglabs/baud/schema"
)

type DBInfo struct {
	schema schema.Database
	spaces map[string]SpaceInfo
}

type SpaceInfo struct {
	partitions []*PartitionInfo
}

type PartitionInfo struct {
	id       schema.PartitionID
	replicas []ReplicaInfo

	leftCh  *PartitionInfo
	rightCh *PartitionInfo
}

type ReplicaInfo struct {
	role string

	zone string
	ip   string
	port string
}

type RouterInfo struct {
	ip   string
	port string

	cpu int
	mem int
}

type ZoneInfo struct {
	region  string
	id      string
	routers []RouterInfo
}
