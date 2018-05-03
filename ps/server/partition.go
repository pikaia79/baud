package server

import (
	"context"
	"sync"

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

type partition struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	server    *Server
	store     kernel.Engine
	closeOnce sync.Once

	rwMutex    sync.RWMutex
	leader     uint64
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
	dataPath, raftPath, err := getDataAndRaftPath(p.meta.ID, p.server.Config.DataPath)
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
	apply, err := p.store.GetLastApplyID()
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
		p.store.Close()
	})

	return nil
}

func (p *partition) getPartitionInfo() *masterpb.PartitionInfo {
	p.rwMutex.RLock()
	info := new(masterpb.PartitionInfo)
	info.ID = p.meta.ID
	info.Status = p.meta.Status
	info.Epoch = p.meta.Epoch
	info.Statistics = p.statistics
	p.rwMutex.RUnlock()

	return info
}

func (p *partition) validate() *metapb.Error {
	p.rwMutex.RLock()
	defer p.rwMutex.RUnlock()

	if p.meta.Status == metapb.PA_INVALID || p.meta.Status == metapb.PA_NOTREAD {
		return &metapb.Error{PartitionNotFound: &metapb.PartitionNotFound{p.meta.ID}}
	}
	if p.leader == 0 {
		return &metapb.Error{NoLeader: &metapb.NoLeader{p.meta.ID}}
	}
	if p.leader != uint64(p.server.nodeID) {
		return &metapb.Error{NotLeader: &metapb.NotLeader{
			PartitionID: p.meta.ID,
			NodeID:      metapb.NodeID(p.leader),
			NodeAddr:
		}}
	}
}
