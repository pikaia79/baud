package master

import (
	"math/rand"
	"time"
)

type Selector interface {
	SelectTarget(servers []*PartitionServer) *PartitionServer
}

type IdleSelector struct {
}

func NewIdleSelector() Selector {
	rand.Seed(time.Now().UnixNano())
	return &IdleSelector{}
}

func (s *IdleSelector) SelectTarget(servers []*PartitionServer) *PartitionServer {
	if servers == nil || len(servers) == 0 {
		return nil
	}

	startIdx := rand.Intn(len(servers))

	return servers[startIdx]
}
