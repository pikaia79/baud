package master

import (
	"context"
	"time"
	"util/log"
	"fmt"
)

const (
	PROCESSOR_DEFAULT_TIMEOUT = time.Second * 60
	PROCESSOR_DEFAULT_CHANNEL_LIMIT = 1000
)

var (
	ProcessorPartitionCh chan*Partition
)

type PartitionCreateProcessor struct {
	ctx         context.Context
	cancelFunc  context.CancelFunc
	timeout     time.Duration

	cluster  *Cluster
	serverSelector Selector
}

func NewPartitionCreateProcessor(cluster *Cluster) *PartitionCreateProcessor {
	ProcessorPartitionCh = make(chan *Partition, PROCESSOR_DEFAULT_CHANNEL_LIMIT)
	p := &PartitionCreateProcessor{
		cluster:        cluster,
		serverSelector: NewIdleSelector(),
	}
	p.ctx, p.cancelFunc = context.WithCancel(context.Background())

	return p
}

func PushMsg(msg interface{}) error {
	if len(ProcessorPartitionCh) >= PROCESSOR_DEFAULT_CHANNEL_LIMIT * 0.9 {
		log.Error("partition create processor will full, reject msg[%v]", msg)
		return ErrSysBusy
	}

	ProcessorPartitionCh <- msg.(*Partition)

	return nil
}

func (p *PartitionCreateProcessor) Run() {
	go func() {
		psRpcClient := GetPSRpcClientInstance()
		idGenerator := GetIdGeneratorInstance(nil)

		for {
			select {
			case <-p.ctx.Done():
				return
			case partitionToCreate := <-ProcessorPartitionCh:

				server := p.serverSelector.SelectTarget(p.cluster.psCache.getAllPartitionServers())
				if server == nil {
					log.Error("Do not distribute suitable server")
					// TODO: calling jdos api to allocate a container asynchronously
					break
				} else {
					addr := fmt.Sprintf("%s:%s", server.Ip, server.Port)
					replicaId, err := idGenerator.GenID()
					if err != nil {
						log.Error("fail to generate new replicaid. err:[%v]", err)
						break
					}

					servers := partitionToCreate.replicaGroup.getAllServers()
					if len(servers) >= FIXED_REPLICA_NUM {
						log.Error("!!!Cannot add new replica, because replica numbers[%v] of partition[%v]",
							" exceed fixed replica number[%v]", len(servers), partitionToCreate, FIXED_REPLICA_NUM)
						break
					}
					servers = append(servers, server)
					if err := psRpcClient.CreateReplica(addr, partitionToCreate.Partition, replicaId, servers); err != nil {
						log.Error("fail to do rpc create replica of partition[%v]. err:[%v]",
							partitionToCreate.Partition, err)
						break
					}
				}
			}
		}
	}()
}

func (p *PartitionCreateProcessor) Stop() {
	if p.cancelFunc != nil {
		p.cancelFunc()
	}
}


