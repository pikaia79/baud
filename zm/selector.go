package zm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"math/rand"
	"time"
)

type Selector interface {
	SelectTarget(servers []*PartitionServer, partitionId metapb.PartitionID) *PartitionServer
}

type IdleSelector struct {
}

func NewIdleSelector() Selector {
	rand.Seed(time.Now().UnixNano())
	return &IdleSelector{}
}

func (s *IdleSelector) SelectTarget(servers []*PartitionServer, partitionId metapb.PartitionID) *PartitionServer {
	if servers == nil || len(servers) == 0 {
		return nil
	}

	candidatePs := make([]*PartitionServer, 0)
	for _, ps := range servers {
		if ps.partitionCache.FindPartitionById(partitionId) != nil {
			continue
		}

		candidatePs = append(candidatePs, ps)
	}
	if len(candidatePs) == 0 {
		return nil
	}

	return candidatePs[rand.Intn(len(candidatePs))]
}
