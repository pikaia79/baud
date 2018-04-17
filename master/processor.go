package master

import (
    "context"
    "time"
    "util/log"
    "fmt"
)

const (
    PROCESSOR_DEFAULT_TIMEOUT       = time.Second * 60
    PROCESSOR_DEFAULT_CHANNEL_LIMIT = 1000

    EVENT_STATUS_FIRST_CREATE = "first_create"
    EVENT_STATUS_WAIT_CONTAINER = "wait_container"
)

const (
    EVENT_TYPE_PARTITION_CREATE = iota
    EVENT_TYPE_PARTITION_DELETE
)

var (
    processorStarted = false
    partitionCreateProcessor *PartitionCreateProcessor
)

type ProcessorEvent struct {
    typ    int
    status string
    body   interface{}
}

type PartitionCreateProcessor struct {
    ctx        context.Context
    cancelFunc context.CancelFunc

    PartitionEventCh chan *Partition
    timeout          time.Duration

    cluster        *Cluster
    serverSelector Selector
    jdos           DCOS
}

func createPartitionCreateProcessor(cluster *Cluster) {
    p := &PartitionCreateProcessor{
        PartitionEventCh: make(chan *Partition, PROCESSOR_DEFAULT_CHANNEL_LIMIT),
        timeout:          PROCESSOR_DEFAULT_TIMEOUT,
        cluster:          cluster,
        serverSelector:   NewIdleSelector(),
        jdos:             new(JDOS),
    }
    p.ctx, p.cancelFunc = context.WithCancel(context.Background())

    partitionCreateProcessor = p
}

// background partitionCreateProcessor
func ProcessorStart(cluster *Cluster) {
    createPartitionCreateProcessor(cluster)
    go func() {
        partitionCreateProcessor.doRun()
    }()

    processorStarted = true
}

func ProcessorStop() {
    if partitionCreateProcessor != nil && partitionCreateProcessor.cancelFunc != nil {
        partitionCreateProcessor.cancelFunc()
    }

    processorStarted = false
}

func PushProcessorEvent(event *ProcessorEvent) error {
    if event == nil {
        return ErrInternalError
    }

    switch event.typ {
    case EVENT_TYPE_PARTITION_CREATE:
        if len(partitionCreateProcessor.PartitionEventCh) >= PROCESSOR_DEFAULT_CHANNEL_LIMIT * 0.9 {
            log.Error("partition create partitionCreateProcessor will full, reject msg[%v]", msg)
            return ErrSysBusy
        }

        partitionCreateProcessor.PartitionEventCh <- event.body.(*Partition)

    case EVENT_TYPE_PARTITION_DELETE:
    default:
        log.Error("processor received invalid event type[%v]", event.typ)
        return ErrInternalError
    }

    return nil
}

func (p *PartitionCreateProcessor) doRun() {
    psRpcClient := GetPSRpcClientInstance()
    idGenerator := GetIdGeneratorInstance(nil)

    for {
        select {
        case <-p.ctx.Done():
            return
        case partitionToCreate := <-p.PartitionEventCh:

            server := p.serverSelector.SelectTarget(p.cluster.psCache.getAllServers())
            if server == nil {
                log.Error("Can not distribute suitable server")
                // TODO: calling jdos api to allocate a container asynchronously
                break
            } else {
                addr := fmt.Sprintf("%s:%s", server.Ip, server.Port)
                replicaId, err := idGenerator.GenID()
                if err != nil {
                    log.Error("fail to generate new replica ßid. err:[%v]", err)
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
                // new replica created do persistent when ps sent heartbeat up
            }
        }
    }
}
