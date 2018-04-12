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
	return &IdleSelector{
	}
}

func (s *IdleSelector) SelectTarget(servers []*PartitionServer) *PartitionServer {
	if servers == nil {
		return nil
	}

	// prevent form selecting same one server at many times
	startIdx := rand.Intn(len(servers))
	for idx := startIdx; idx < len(servers); idx++ {
		server := servers[idx]
		if !server.isReplicaFull() {
			return server
		}
	}
	for idx := 0; idx < startIdx; idx++ {
		server := servers[idx]
		if !server.isReplicaFull() {
			return server
		}
	}

	return nil
}
