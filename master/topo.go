package master

import (
	. "github.com/tiglabs/baud/schema"
)

type DBInfo struct {
	schema Graph
	spaces map[string]SpaceInfo
}

type SpaceInfo struct {
	partitions []*PartitionInfo
}

type PartitionInfo struct {
	id       PartitionID
	replicas []ReplicaInfo

	leftCh  *PartitionInfo
	rightCh *PartitionInfo

	status string //serving, splitting, cleaning, etc.
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
