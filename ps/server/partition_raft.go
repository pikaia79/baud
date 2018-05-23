package server

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb/raftpb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

func (p *partition) Apply(command []byte, index uint64) (resp interface{}, err error) {
	raftCmd := raftpb.CreateRaftCommand()
	if err = raftCmd.Unmarshal(command); err != nil {
		panic(err)
	}

	switch raftCmd.Type {
	case raftpb.CmdType_WRITE:
		resp, err = p.execWriteCommand(index, raftCmd.WriteCommands)

	default:
		p.store.SetApplyID(index)
		err = errorPartitonCommand
		log.Error("unsupported command[%s]", raftCmd.Type)
	}

	raftCmd.Close()
	return
}

func (p *partition) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	p.rwMutex.Lock()
	switch confChange.Type {
	case proto.ConfAddNode:
		for _, r := range p.meta.Replicas {
			if confChange.Peer.ID == uint64(r.NodeID) {
				goto ret
			}
		}

		replica := new(metapb.Replica)
		replica.Unmarshal(confChange.Context)
		p.server.nodeResolver.addNode(replica.NodeID, replica.ReplicaAddrs)

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

ret:
	p.rwMutex.Unlock()
	return nil, nil
}

func (p *partition) Snapshot() (proto.Snapshot, error) {
	return nil, nil
}

func (p *partition) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	return nil
}

func (p *partition) HandleLeaderChange(leader uint64) {
	p.rwMutex.Lock()

	if p.leader != leader {
		log.Debug("partition[%d] change leader from %d to %d", p.meta.ID, p.leader, leader)

		p.leader = leader
		p.leaderAddr = ""
		for _, repl := range p.meta.Replicas {
			if uint64(repl.NodeID) == p.leader {
				p.leaderAddr = repl.RpcAddr
				break
			}
		}

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
	}

	p.rwMutex.Unlock()
	return
}

func (p *partition) HandleFatalEvent(err *raft.FatalError) {
	log.Error("partition[%d] occur fatal error: %v", p.meta.ID, err.Err)
	p.Close()
	p.server.masterHeartbeat.trigger()
}
