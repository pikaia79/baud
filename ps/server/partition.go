package server

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/tiglabs/baudengine/kernel"
	"github.com/tiglabs/baudengine/kernel/index"
	"github.com/tiglabs/baudengine/kernel/store/kvstore/badgerdb"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

var (
	errorPartitonCommand = errors.New("unsupported command")
)

type partition struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	server    *Server
	store     kernel.Engine
	closeOnce sync.Once

	rwMutex    sync.RWMutex
	leader     uint64
	leaderAddr string
	meta       metapb.Partition
	statistics masterpb.PartitionStats
}

func newPartition(server *Server, meta metapb.Partition) *partition {
	p := &partition{
		meta:   meta,
		server: server,
	}
	p.meta.Status = metapb.PA_NOTREAD
	p.ctx, p.ctxCancel = context.WithCancel(server.ctx)

	return p
}

func (p *partition) start() {
	// create and open store engine
	dataPath, raftPath, err := p.server.meta.getDataAndRaftPath(p.meta.ID)
	if err != nil {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()
		log.Error("start partition[%d] create data and raft path error: %v", p.meta.ID, err)
		return
	}

	storeOpt := &badgerdb.StoreConfig{
		Path:     dataPath,
		Sync:     false,
		ReadOnly: false,
	}
	kvStore, err := badgerdb.New(storeOpt)
	if err != nil {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()
		log.Error("start partition[%d] open store engine error: %v", p.meta.ID, err)
		return
	}
	p.store = index.NewIndexDriver(kvStore)
	apply, err := p.store.GetApplyID()
	if err != nil {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()
		p.store.Close()
		log.Error("start partition[%d] get last apply index error: %v", p.meta.ID, err)
		return
	}

	// create and open raft replication
	raftStore, err := wal.NewStorage(raftPath, nil)
	if err != nil {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()
		p.store.Close()
		log.Error("start partition[%d] open raft store engine error: %v", p.meta.ID, err)
		return
	}

	raftConf := &raft.RaftConfig{
		ID:           p.meta.ID,
		Applied:      apply,
		Peers:        make([]proto.Peer, 0, len(p.meta.Replicas)),
		Storage:      raftStore,
		StateMachine: p,
	}
	for _, r := range p.meta.Replicas {
		peer := proto.Peer{Type: proto.PeerNormal, ID: uint64(r.NodeID)}
		raftConf.Peers = append(raftConf.Peers, peer)
	}
	if p.server.raftServer.CreateRaft(raftConf); err != nil {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()
		p.store.Close()
		log.Error("start partition[%d] create raft error: %v", p.meta.ID, err)
		return
	}

	p.rwMutex.Lock()
	p.meta.Status = metapb.PA_READONLY
	p.rwMutex.Unlock()
	log.Info("start partition[%d] success", p.meta.ID)
	return
}

func (p *partition) Close() error {
	p.closeOnce.Do(func() {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()

		p.ctxCancel()
		p.server.raftServer.RemoveRaft(p.meta.ID)
		if p.store != nil {
			p.store.Close()
		}
	})

	return nil
}

func (p *partition) getPartitionInfo() *masterpb.PartitionInfo {
	p.rwMutex.RLock()
	info := new(masterpb.PartitionInfo)
	info.ID = p.meta.ID
	info.IsLeader = (p.leader == uint64(p.server.nodeID))
	info.Status = p.meta.Status
	info.Epoch = p.meta.Epoch
	info.Statistics = p.statistics
	replicas := p.meta.Replicas
	p.rwMutex.RUnlock()

	if info.IsLeader {
		raftStatus := p.server.raftServer.Status(p.meta.ID)
		info.RaftStatus = new(masterpb.RaftStatus)
		for _, r := range replicas {
			if r.NodeID == p.server.nodeID {
				info.RaftStatus.Replica = r
				info.RaftStatus.Term = raftStatus.Term
				info.RaftStatus.Index = raftStatus.Index
				info.RaftStatus.Commit = raftStatus.Commit
				info.RaftStatus.Applied = raftStatus.Applied
			} else {
				if replStatus, ok := raftStatus.Replicas[uint64(r.NodeID)]; ok {
					follower := masterpb.RaftFollowerStatus{
						Replica: r,
						Match:   replStatus.Match,
						Commit:  replStatus.Commit,
						Next:    replStatus.Next,
						State:   replStatus.State,
					}
					since := time.Since(replStatus.LastActive)
					// 两次心跳内没活跃就视为Down
					downDuration := since - time.Duration(2*p.server.raftConfig.HeartbeatTick)*p.server.raftConfig.TickInterval
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

func (p *partition) checkReadable(readLeader bool) (err *metapb.Error) {
	p.rwMutex.RLock()

	if p.meta.Status == metapb.PA_INVALID || p.meta.Status == metapb.PA_NOTREAD {
		err = &metapb.Error{PartitionNotFound: &metapb.PartitionNotFound{p.meta.ID}}
		goto ret
	}
	if p.leader == 0 {
		err = &metapb.Error{NoLeader: &metapb.NoLeader{p.meta.ID}}
		goto ret
	}
	if readLeader && p.leader != uint64(p.server.nodeID) {
		err = &metapb.Error{NotLeader: &metapb.NotLeader{
			PartitionID: p.meta.ID,
			Leader:      metapb.NodeID(p.leader),
			LeaderAddr:  p.leaderAddr,
			Epoch:       p.meta.Epoch,
		}}
		goto ret
	}

ret:
	p.rwMutex.RUnlock()
	return
}
