package server

import (
	"context"

	"github.com/tiglabs/baud/kernel"
	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
)

type partition struct {
	meta metapb.Partition
	ctx  context.Context
	quit context.CancelFunc

	server *Server
	store  kernel.Engine
}

func newPartition(server *Server, meta metapb.Partition) *partition {
	p := &partition{
		meta:   meta,
		server: server,
	}
	p.meta.Status = metapb.PA_Invalid
	p.ctx, p.quit = context.WithCancel(server.ctx)

	return p
}

func (p *partition) start() {

}

func (p *partition) getPartitionInfo() *masterpb.PartitionInfo {
	return nil
}

func (p *partition) Close() error {
}
