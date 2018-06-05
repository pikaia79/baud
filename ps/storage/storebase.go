package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/ps/server"
)

var (
	ErrorTimeout = new(metapb.TimeoutError)
	ErrorCommand = errors.New("unsupported command")
)

// StoreBase is the base class of partition store.
type StoreBase struct {
	sync.RWMutex
	Ctx       context.Context
	CtxCancel context.CancelFunc

	Server    *server.Server
	Engine    engine.Engine
	CloseOnce sync.Once

	Meta  metapb.Partition
	Stats masterpb.PartitionStats
}

func (s *StoreBase) GetMeta() (meta metapb.Partition) {
	s.RLock()
	meta = s.Meta
	s.RUnlock()
	return
}
