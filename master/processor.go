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

type Processor interface {
	PushMsg(interface{}) error
	Run()
}

type SpaceCreateProcessor struct {
	ctx 	context.Context
	ctxCancel context.CancelFunc
	timeout 	time.Duration
	spaceCh   chan*Space

	cluster  *Cluster
	serverSelector Selector
}

func NewSpaceCreateProcessor(cluster *Cluster) Processor {
	p := new(SpaceCreateProcessor)
	p.ctx, p.ctxCancel = context.WithCancel(context.Background())
	p.spaceCh = make(chan *Space, PROCESSOR_DEFAULT_CHANNEL_LIMIT)
	p.cluster = cluster
	p.serverSelector = NewIdleSelector()
	return p
}

func (p *SpaceCreateProcessor) PushMsg(msg interface{}) error {
	if len(p.spaceCh) >= PROCESSOR_DEFAULT_CHANNEL_LIMIT * 0.9 {
		log.Error("space create processor will full, rejected the space[%v] request", space)
		return ErrSysBusy
	}

	p.spaceCh <- msg.(*Space)

	return nil
}

func (p *SpaceCreateProcessor) Run() {

	for {
		var space *Space
		select {
		case <-p.ctx.Done():
			return
		case space = <-p.spaceCh:
			numPartitions := space.Partitioning.NumPartitions

			server := p.serverSelector.SelectTarget(p.cluster.psCache.getAllPartitionServers())


		}
	}
}


