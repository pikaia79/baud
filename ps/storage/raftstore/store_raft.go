package raftstore

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb/raftpb"
	"github.com/tiglabs/baudengine/ps/storage"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

// Apply implements the raft interface.
func (s *Store) Apply(command []byte, index uint64) (resp interface{}, err error) {
	raftCmd := raftpb.CreateRaftCommand()
	if err = raftCmd.Unmarshal(command); err != nil {
		panic(err)
	}

	switch raftCmd.Type {
	case raftpb.CmdType_WRITE:
		resp, err = s.execRaftCommand(index, raftCmd.WriteCommands)

	default:
		s.Engine.SetApplyID(index)
		err = storage.ErrorCommand
		log.Error("unsupported command[%s]", raftCmd.Type)
	}

	raftCmd.Close()
	return
}

// ApplyMemberChange implements the raft interface.
func (s *Store) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (interface{}, error) {
	s.Lock()
	switch confChange.Type {
	case proto.ConfAddNode:
		for _, r := range s.Meta.Replicas {
			if confChange.Peer.ID == uint64(r.NodeID) {
				goto ret
			}
		}

		replica := new(metapb.Replica)
		replica.Unmarshal(confChange.Context)
		s.EventListener.HandleRaftReplicaEvent(&RaftReplicaEvent{Replica: replica})

		s.Meta.Epoch.ConfVersion++
		s.Meta.Replicas = append(s.Meta.Replicas, *replica)

	case proto.ConfRemoveNode:
		replica := new(metapb.Replica)
		replica.Unmarshal(confChange.Context)
		s.EventListener.HandleRaftReplicaEvent(&RaftReplicaEvent{Delete: true, Replica: replica})

		s.Meta.Epoch.ConfVersion++
		replicas := make([]metapb.Replica, 0, len(s.Meta.Replicas)-1)
		for _, r := range s.Meta.Replicas {
			if r.ID != replica.ID {
				replicas = append(replicas, r)
			}
		}
		s.Meta.Replicas = replicas

	default:

	}

ret:
	s.Unlock()
	return nil, nil
}

// Snapshot implements the raft interface.
func (s *Store) Snapshot() (proto.Snapshot, error) {
	return nil, nil
}

// ApplySnapshot implements the raft interface.
func (s *Store) ApplySnapshot(peers []proto.Peer, iter proto.SnapIterator) error {
	return nil
}

// HandleLeaderChange implements the raft interface.
func (s *Store) HandleLeaderChange(leader uint64) {
	s.Lock()

	if s.Leader != leader {
		log.Debug("partition[%d] change leader from %d to %d", s.Meta.ID, s.Leader, leader)

		s.Leader = leader
		s.LeaderAddr = ""
		for _, repl := range s.Meta.Replicas {
			if uint64(repl.NodeID) == s.Leader {
				s.LeaderAddr = repl.RpcAddr
				break
			}
		}

		_, term := s.RaftServer.LeaderTerm(s.Meta.ID)
		if s.Meta.Epoch.Version < term {
			s.Meta.Epoch.Version = term
		}

		if leader == uint64(s.NodeID) {
			s.Meta.Status = metapb.PA_READWRITE
			s.EventListener.HandleRaftLeaderEvent(&RaftLeaderEvent{Store: s})
		} else {
			s.Meta.Status = metapb.PA_READONLY
		}
	}

	s.Unlock()
	return
}

// HandleFatalEvent implements the raft interface.
func (s *Store) HandleFatalEvent(err *raft.FatalError) {
	log.Error("partition[%d] occur fatal error: %s", s.Meta.ID, err.Err)
	s.Close()
	s.EventListener.HandleRaftFatalEvent(&RaftFatalEvent{
		Store: s,
		Cause: err.Err,
	})
}
