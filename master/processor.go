package master

import (
    "context"
    "util/log"
    "github.com/tiglabs/baud/proto/metapb"
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
    partitionId metapb.PartitionID
    leaderNodeId metapb.NodeID
    replica *metapb.Replica
}
func NewPartitionDeleteEvent(partitionId metapb.PartitionID, leaderNodeId metapb.NodeID,
        replica *metapb.Replica) *ProcessorEvent {
    return &ProcessorEvent{
        typ:  EVENT_TYPE_PARTITION_DELETE,
        body: &PartitionDeleteBody {
            partitionId:partitionId,
            leaderNodeId:leaderNodeId,
            replica:   replica,
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
                    leaderReplica := partitionToCreate.pickLeaderReplica()
                    leaderPS := p.cluster.psCache.findServerById(leaderReplica.NodeID)
                    if leaderPS == nil {
                        log.Debug("can not find leader ps when notify adding replicas to leader")
                        return
                    }

                    replicaId, err := idGenerator.GenID()
                    if err != nil {
                        log.Error("fail to generate new replica ßid. err:[%v]", err)
                        return
                    }
                    var newMetaReplica = &metapb.Replica{ID: metapb.ReplicaID(replicaId), NodeID: destPS.ID}


                    partitionCopy := deepcopy.Iface(partitionToCreate.Partition).(*metapb.Partition)
                    partitionCopy.Replicas = append(partitionCopy.Replicas, *newMetaReplica)
                    if err := psRpcClient.CreatePartition(destPS.getRpcAddr(), partitionCopy); err != nil {
                        log.Error("Rpc fail to create partition[%v] into ps. err:[%v]",
                            partitionToCreate.Partition, err)
                       return
                    }

                    if err := psRpcClient.AddReplica(leaderPS.getRpcAddr(), partitionToCreate.ID, &destPS.RaftAddrs,
                            newMetaReplica.ID, newMetaReplica.NodeID); err != nil {
                        log.Error("Rpc fail to add replica[%v] into leader ps. err[%v]", newMetaReplica, err)
                        return
                    }
                    //
                    //if err := partitionToCreate.addReplica(p.cluster.store, newMetaReplica); err != nil {
                    //    log.Error("partition add new replica err[%v]", err)
                    //    return
                    //}

                }(event.body.(*Partition))

            } else if event.typ == EVENT_TYPE_PARTITION_DELETE {

                body := event.body.(*PartitionDeleteBody)

                go func(partitionId metapb.PartitionID, leaderNodeId metapb.NodeID, replica *metapb.Replica) {
                    leaderPS := p.cluster.psCache.findServerById(leaderNodeId)
                    if leaderPS == nil {
                        log.Debug("can not find leader ps when notify deleting replicas to leader")
                        return
                    }
                    destPS := p.cluster.psCache.findServerById(replica.NodeID)
                    if destPS == nil {
                        log.Debug("can not find replica[%v] ps needed to deleted", replica.NodeID)
                        return
                    }


                    if err := psRpcClient.RemoveReplica(leaderPS.getRpcAddr(), partitionId, &destPS.RaftAddrs,
                            replica.ID, replica.NodeID); err != nil {
                        log.Error("Rpc fail to remove replica[%v] from ps. err[%v]", replica.ID, err)
                        return
                    }

                    if err := psRpcClient.DeletePartition(destPS.getRpcAddr(), partitionId);
                            err != nil {
                        log.Error("Rpc fail to delete partition[%v] from ps. err:[%v]", partitionId, err)
                        return
                    }
                    //
                    //if err := partitionToDelete.deleteReplica(p.cluster.store, replicaToDelete); err != nil {
                    //    log.Error("partition delete replica err[%v]", err)
                    //    return
                    //}

                }(body.partitionId, body.leaderNodeId, body.replica)
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
