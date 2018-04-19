package master

import (
    "context"
    "util/log"
    "util"
    "proto/metapb"
)

const (
    PARTITION_CHANNEL_LIMIT = 1000
)

const (
    EVENT_TYPE_INVALID = iota
    EVENT_TYPE_PARTITION_CREATE
    EVENT_TYPE_PARTITION_DELETE
)

var (
    processorStarted = false

    partitionProcessor *PartitionProcessor
)

type ProcessorEvent struct {
    typ    int
    body   interface{}
}

func NewPartitionCreateEvent(partition *Partition) *ProcessorEvent {
    return &ProcessorEvent{
        typ:  EVENT_TYPE_PARTITION_CREATE,
        body: partition,
    }
}

func NewPartitionDeleteEvent(partition *Partition) *ProcessorEvent {
    return &ProcessorEvent{
        typ:  EVENT_TYPE_PARTITION_DELETE,
        body: partition,
    }
}

type PartitionProcessor struct {
    ctx        context.Context
    cancelFunc context.CancelFunc

    eventCh chan *ProcessorEvent

    cluster        *Cluster
    serverSelector Selector
    jdos           DCOS
}


func (p *PartitionProcessor) doRun() {
    psRpcClient := GetPSRpcClientInstance()
    idGenerator := GetIdGeneratorInstance(nil)

    for {
        select {
        case <-p.ctx.Done():
            return
        case event := <-p.eventCh:

            if event.typ == EVENT_TYPE_PARTITION_CREATE {
                server := p.serverSelector.SelectTarget(p.cluster.psCache.getAllServers())
                if server == nil {
                    log.Error("Can not distribute suitable server")
                    // TODO: calling jdos api to allocate a container asynchronously
                    break
                }

                go func(partitionToCreate *Partition) {
                    addr := util.BuildAddr(server.Ip, server.Port)
                    replicaId, err := idGenerator.GenID()
                    if err != nil {
                        log.Error("fail to generate new replica ßid. err:[%v]", err)
                        return
                    }

                    //servers := partitionReceived.replicaGroup.getAllServers()
                    //if len(servers) >= FIXED_REPLICA_NUM {
                    //    log.Error("!!!Cannot add new replica, because replica numbers[%v] of partition[%v]",
                    //        " exceed fixed replica number[%v]", len(servers), partitionReceived, FIXED_REPLICA_NUM)
                    //    break
                    //}
                    //servers = append(servers, server)

                    var newMetaReplica = &metapb.Replica{ID: replicaId}
                    partitionToCreate.addReplica(newMetaReplica)
                    if err := psRpcClient.CreateReplica(addr, partitionToCreate.Partition); err != nil {
                        log.Error("fail to do rpc create replica of partition[%v]. err:[%v]",
                            partitionToCreate.Partition, err)
                       return
                    }

                    // TODO: updatePartition
                }(event.body.(*Partition))

            } else if event.typ == EVENT_TYPE_PARTITION_DELETE {

                go func() {

                }()
            }
        }
    }
}

func createPartitionProcessor(cluster *Cluster) {
    p := &PartitionProcessor{
        eventCh:        make(chan *ProcessorEvent, PARTITION_CHANNEL_LIMIT),
        cluster:        cluster,
        serverSelector: NewIdleSelector(),
        jdos:           new(JDOS),
    }
    p.ctx, p.cancelFunc = context.WithCancel(context.Background())

    partitionProcessor = p
}

type PartitionDeleteProcessor struct {
    ctx        context.Context
    cancelFunc context.CancelFunc

    PartitionEventCh chan *Partition
}

// background partitionProcessor
func ProcessorStart(cluster *Cluster) {
    createPartitionProcessor(cluster)
    go func() {
        partitionProcessor.doRun()
    }()

    processorStarted = true
}

func ProcessorStop() {
    if partitionProcessor != nil && partitionProcessor.cancelFunc != nil {
        partitionProcessor.cancelFunc()
    }

    processorStarted = false
}

func PushProcessorEvent(event *ProcessorEvent) error {
    if event == nil {
        return ErrInternalError
    }

    if event.typ == EVENT_TYPE_PARTITION_CREATE || event.typ == EVENT_TYPE_PARTITION_DELETE {
        if len(partitionProcessor.eventCh) >= PARTITION_CHANNEL_LIMIT*0.9 {
            log.Error("partition channel will full, reject event[%v]", event)
            return ErrSysBusy
        }

        partitionProcessor.eventCh <- event

    } else {
        log.Error("processor received invalid event type[%v]", event.typ)
        return ErrInternalError
    }

    return nil
}
