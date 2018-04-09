package master

import (
	"context"
	"time"
	"util/log"
)

const (
	PROCESSOR_DEFAULT_TIMEOUT = time.Second * 60
	PROCESSOR_DEFAULT_CHANNEL_LIMIT = 1000
)

type Processor struct {
	ctx 	context.Context
	ctxCancel context.CancelFunc
	timeout 	time.Duration
	spaceCh   chan*Space

	cluster  *Cluster
}

func NewProcessor(cluster *Cluster) *Processor {
	p := new(Processor)
	p.ctx, p.ctxCancel = context.WithCancel(context.Background())
	p.spaceCh = make(chan *Space, PROCESSOR_DEFAULT_CHANNEL_LIMIT)
	p.cluster = cluster

	return p
}

func (p *Processor) pushSpace(space *Space) error {
	if len(p.spaceCh) >= PROCESSOR_DEFAULT_CHANNEL_LIMIT * 0.9 {
		log.Error("space create processor will full, rejected the space[%v] request", space)
		return ErrSysBusy
	}

	p.spaceCh <- space

	return nil
}

func (p *Processor) run() {
	for {
		var space *Space
		select {
		case <-p.ctx.Done():
			return
		case space = <-p.spaceCh:
			numPartitions := space.Partitioning.NumPartitions
			var selectedNode *PartitionServer
			nodes := p.cluster.psCache.getAllPartitionServers()
			for _, node := range nodes {
				if node.
			}

		}


	}
}

