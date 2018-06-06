package raftstore

import (
	"context"
	"time"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/ps/storage"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

// Store is a contiguous slotspace with writes managed via an instance of the Raft consensus algorithm.
type Store struct {
	storage.StoreBase
	RaftPath      string
	Leader        uint64
	LeaderAddr    string
	RaftConfig    *raft.Config
	RaftServer    *raft.RaftServer
	EventListener EventListener
}

type StoreConfig struct {
	engine.EngineConfig
	EngineName string
	Meta       metapb.Partition
	NodeID     metapb.NodeID

	RaftPath   string
	RaftConfig *raft.Config
	RaftServer *raft.RaftServer

	EventListener EventListener
}

// CreateStore create an instance of Store.
func CreateStore(ctx context.Context, conf *StoreConfig) *Store {
	s := new(Store)
	s.NodeID = conf.NodeID
	s.EngineName = conf.EngineName
	s.EngineConf = conf.EngineConfig
	s.Meta = conf.Meta
	s.RaftPath = conf.RaftPath
	s.RaftConfig = conf.RaftConfig
	s.RaftServer = conf.RaftServer
	s.EventListener = conf.EventListener
	s.Ctx, s.CtxCancel = context.WithCancel(ctx)
	s.Meta.Status = metapb.PA_NOTREAD

	return s
}

// Start start the store.
func (s *Store) Start() {
	engine, err := engine.Build(s.EngineName, s.EngineConf)
	if err != nil {
		s.Lock()
		s.Meta.Status = metapb.PA_INVALID
		s.Unlock()
		log.Error("start partition[%d] open store engine error: %s", s.Meta.ID, err)
		return
	}
	s.Engine = engine
	apply, err := s.Engine.GetApplyID()
	if err != nil {
		s.Lock()
		s.Meta.Status = metapb.PA_INVALID
		s.Unlock()
		s.Engine.Close()
		s.Engine = nil
		log.Error("start partition[%d] get last apply index error: %s", s.Meta.ID, err)
		return
	}

	// create and open raft replication
	raftStore, err := wal.NewStorage(s.RaftPath, nil)
	if err != nil {
		s.Lock()
		s.Meta.Status = metapb.PA_INVALID
		s.Unlock()
		s.Engine.Close()
		s.Engine = nil
		log.Error("start partition[%d] open raft store engine error: %s", s.Meta.ID, err)
		return
	}

	raftConf := &raft.RaftConfig{
		ID:           s.Meta.ID,
		Applied:      apply,
		Peers:        make([]proto.Peer, 0, len(s.Meta.Replicas)),
		Storage:      raftStore,
		StateMachine: s,
	}
	for _, repl := range s.Meta.Replicas {
		peer := proto.Peer{Type: proto.PeerNormal, ID: uint64(repl.NodeID)}
		raftConf.Peers = append(raftConf.Peers, peer)
	}
	if s.RaftServer.CreateRaft(raftConf); err != nil {
		s.Lock()
		s.Meta.Status = metapb.PA_INVALID
		s.Unlock()
		s.Engine.Close()
		s.Engine = nil
		log.Error("start partition[%d] create raft error: %s", s.Meta.ID, err)
		return
	}

	s.Lock()
	s.Meta.Status = metapb.PA_READONLY
	s.Unlock()
	log.Info("start partition[%d] success", s.Meta.ID)
}

// Close close store for once
func (s *Store) Close() error {
	s.CloseOnce.Do(func() {
		s.Lock()
		s.Meta.Status = metapb.PA_INVALID
		s.Unlock()

		s.CtxCancel()
		s.RaftServer.RemoveRaft(s.Meta.ID)
		if s.Engine != nil {
			s.Engine.Close()
			s.Engine = nil
		}
	})

	return nil
}

// GetStats returns statistics for store
func (s *Store) GetStats() *masterpb.PartitionInfo {
	s.RLock()
	info := new(masterpb.PartitionInfo)
	info.ID = s.Meta.ID
	info.IsLeader = (s.Leader == uint64(s.NodeID))
	info.Status = s.Meta.Status
	info.Epoch = s.Meta.Epoch
	info.Statistics = s.Stats
	replicas := s.Meta.Replicas
	s.RUnlock()

	if info.IsLeader {
		raftStatus := s.RaftServer.Status(s.Meta.ID)
		info.RaftStatus = new(masterpb.RaftStatus)
		for _, repl := range replicas {
			if repl.NodeID == s.NodeID {
				info.RaftStatus.Replica = repl
				info.RaftStatus.Term = raftStatus.Term
				info.RaftStatus.Index = raftStatus.Index
				info.RaftStatus.Commit = raftStatus.Commit
				info.RaftStatus.Applied = raftStatus.Applied
			} else {
				if replStatus, ok := raftStatus.Replicas[uint64(repl.NodeID)]; ok {
					follower := masterpb.RaftFollowerStatus{
						Replica: repl,
						Match:   replStatus.Match,
						Commit:  replStatus.Commit,
						Next:    replStatus.Next,
						State:   replStatus.State,
					}
					since := time.Since(replStatus.LastActive)
					// 两次心跳内没活跃就视为Down
					downDuration := since - time.Duration(2*s.RaftConfig.HeartbeatTick)*s.RaftConfig.TickInterval
					if downDuration > 0 {
						follower.DownSeconds = uint64(downDuration / time.Second)
					}
					info.RaftStatus.Followers = append(info.RaftStatus.Followers, follower)
				}
			}
		}
	}

	return info
}
