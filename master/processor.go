package master

import (
    "context"
    "util/log"
    "proto/metapb"
    "util/deepcopy"
)

const (
    PARTITION_CHANNEL_LIMIT = 1000
)

const (
    EVENT_TYPE_INVALID              = iota
    EVENT_TYPE_PARTITION_CREATE
    EVENT_TYPE_PARTITION_DELETE
    //EVENT_TYPE_REPLICA_ADD
    //EVENT_TYPE_REPLICA_REMOVE
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

// internal use
type PartitionDeleteBody struct {
    partitionToDelete *Partition
    replicaToDelete   *metapb.Replica
}
func NewPartitionDeleteEvent(partition *Partition, replica *metapb.Replica) *ProcessorEvent {
    return &ProcessorEvent{
        typ:  EVENT_TYPE_PARTITION_DELETE,
        body: &PartitionDeleteBody {
            partitionToDelete: partition,
            replicaToDelete:   replica,
        },
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
                destPS := p.serverSelector.SelectTarget(p.cluster.psCache.getAllServers())
                if destPS == nil {
                    log.Error("Can not distribute suitable ps")
                    // TODO: calling jdos api to allocate a container asynchronously
                    break
                }

                go func(partitionToCreate *Partition) {
                    replicaId, err := idGenerator.GenID()
                    if err != nil {
                        log.Error("fail to generate new replica ßid. err:[%v]", err)
                        return
                    }

                    // TODO: to prevent from the new replica as garbage is removed, in the case replica
                    // when ps heartbeat report.
                    // Secondly create
                    // Finally notify new member to the raft leader, partition info has already included leader address.
                    // Finally notify the leader to
                    var newMetaReplica = &metapb.Replica{ID: replicaId, NodeID: destPS.ID}

                    partitionCopy := deepcopy.Iface(partitionToCreate.Partition).(*metapb.Partition)
                    partitionCopy.Replicas = append(partitionCopy.Replicas, *newMetaReplica)
                    if err := psRpcClient.CreatePartition(destPS.getRpcAddr(), partitionCopy); err != nil {
                        log.Error("Rpc fail to create partition[%v] into ps. err:[%v]",
                            partitionToCreate.Partition, err)
                       return
                    }

                    // notify the leader to new member
                    leaderReplica := partitionToCreate.pickLeaderReplica()
                    leaderPS := p.cluster.psCache.findServerById(leaderReplica.NodeID)
                    if leaderPS == nil {
                        log.Debug("can not find leader ps when notify adding replicas to leader")
                        return
                    }
                    if err := psRpcClient.AddReplica(leaderPS.getRpcAddr(), destPS.Node, partitionToCreate.Partition,
                            newMetaReplica); err != nil {
                        log.Error("Rpc fail to add replica[%v] into leader ps. err[%v]", newMetaReplica, err)
                        return
                    }

                    // only whole progress success, then add new replica into memory and persistent
                    // others replicas will be deleted in background worker process
                    if err := partitionToCreate.addReplica(p.cluster.store, newMetaReplica); err != nil {
                        log.Error("partition add new replica err[%v]", err)
                        return
                    }

                }(event.body.(*Partition))

            } else if event.typ == EVENT_TYPE_PARTITION_DELETE {

                body := event.body.(*PartitionDeleteBody)

                go func(partitionToDelete *Partition, replicaToDelete *metapb.Replica) {
                    // firstly delete the replica from disk, secondly delete it from raft group, finally from ps,
                    // if deleting progress failure, delete it again when ps heartbeat report at next time
                    if err := partitionToDelete.deleteReplica(p.cluster.store, replicaToDelete); err != nil {
                        log.Error("partition delete replica err[%v]", err)
                        return
                    }

                    leaderReplica := partitionToDelete.pickLeaderReplica()
                    leaderPS := p.cluster.psCache.findServerById(leaderReplica.NodeID)
                    if leaderPS == nil {
                        log.Debug("can not find leader ps when notify deleting replicas to leader")
                        return
                    }

                    destPS := p.cluster.psCache.findServerById(replicaToDelete.NodeID)
                    if destPS == nil {
                        log.Debug("can not find replica[%v] ps needed to deleted", replicaToDelete)
                        return
                    }
                    if err := psRpcClient.RemoveReplica(leaderPS.getRpcAddr(), destPS.Node, partitionToDelete.Partition,
                            replicaToDelete); err != nil {
                        log.Error("Rpc fail to remove replica[%v] from ps. err[%v]", replicaToDelete, err)
                        return
                    }


                }(body.partitionToDelete, body.replicaToDelete)
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
