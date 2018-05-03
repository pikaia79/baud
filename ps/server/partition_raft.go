package server

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

func (p *partition) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	switch confChange.Type {
	case proto.ConfAddNode:
		for _, r := range p.meta.Replicas {
			if confChange.Peer.ID == uint64(r.NodeID) {
				return nil, nil
			}
		}

		replica := new(metapb.Replica)
		replica.Unmarshal(confChange.Context)
		p.server.nodeResolver.addNode(replica.NodeID, replica.RaftAddrs)

		p.meta.Epoch.ConfVersion++
		p.meta.Replicas = append(p.meta.Replicas, *replica)

	case proto.ConfRemoveNode:
		replica := new(metapb.Replica)
		replica.Unmarshal(confChange.Context)
		p.server.nodeResolver.deleteNode(replica.NodeID)

		p.meta.Epoch.ConfVersion++
		replicas := make([]metapb.Replica, 0, len(p.meta.Replicas)-1)
		for _, r := range p.meta.Replicas {
			if r.ID != replica.ID {
				replicas = append(replicas, r)
			}
		}
		p.meta.Replicas = replicas

	default:

	}

	return nil, nil
}

func (p *partition) HandleLeaderChange(leader uint64) {
	p.rwMutex.Lock()
	defer p.rwMutex.Unlock()

	if p.leader == leader {
		return
	}

	p.leader = leader
	_, term := p.server.raftServer.LeaderTerm(p.meta.ID)
	if p.meta.Epoch.Version < term {
		p.meta.Epoch.Version = term
	}

	if leader == uint64(p.server.nodeID) {
		p.meta.Status = metapb.PA_READWRITE
		p.server.masterHeartbeat.trigger()
	} else {
		p.meta.Status = metapb.PA_READONLY
	}
	return
}

func (p *partition) HandleFatalEvent(err *raft.FatalError) {
	log.Error("partition[%d] occur fatal error: %v", p.meta.ID, err.Err)
	p.Close()
	p.server.masterHeartbeat.trigger()
}
