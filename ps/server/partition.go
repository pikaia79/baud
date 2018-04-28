package server

import (
	"context"
	"sync"

	"github.com/tiglabs/baud/kernel"
	"github.com/tiglabs/baud/kernel/index"
	"github.com/tiglabs/baud/kernel/mapping"
	"github.com/tiglabs/baud/kernel/store/kvstore/badgerdb"
	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/util/log"
)

type partition struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	server    *Server
	store     kernel.Engine
	mapping   *mapping.DocumentMapping
	closeOnce sync.Once

	rwMutex    sync.RWMutex
	meta       metapb.Partition
	epoch      metapb.PartitionEpoch
	statistics masterpb.PartitionStats
}

func newPartition(server *Server, meta metapb.Partition, mapping *mapping.DocumentMapping) *partition {
	p := &partition{
		meta:    meta,
		mapping: mapping,
		server:  server,
	}
	p.meta.Status = metapb.PA_NOTREAD
	p.ctx, p.ctxCancel = context.WithCancel(server.ctx)

	return p
}

func (p *partition) start() {
	// create and open store engine
	path, err := getDataPath(p.meta.ID, p.server.Config.DataPath, true)
	if err != nil {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()
		log.Error("start partition[%d] create data path error: %v", p.meta.ID, err)
		return
	}

	storeOpt := &badgerdb.StoreConfig{
		Path:     path,
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
}

func (p *partition) Close() error {
	p.closeOnce.Do(func() {
		p.rwMutex.Lock()
		p.meta.Status = metapb.PA_INVALID
		p.rwMutex.Unlock()

		p.ctxCancel()
		p.store.Close()
	})

	return nil
}

func (p *partition) getPartitionInfo() *masterpb.PartitionInfo {
	p.rwMutex.RLock()
	info := new(masterpb.PartitionInfo)
	info.ID = p.meta.ID
	info.Status = p.meta.Status
	info.Epoch = p.epoch
	info.Statistics = p.statistics
	p.rwMutex.RUnlock()

	return info
}

func (p *partition) validate() *metapb.Error {
	return nil
}
