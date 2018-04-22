package master

import (
	"context"
	"github.com/tiglabs/baud/proto/metapb"
	"util/deepcopy"
	"util/log"
	/*  "util/rpc"
	    "google.golang.org/grpc"
	    "proto/pspb"*/)

const (
	PARTITION_CHANNEL_LIMIT = 1000
)

const (
	EVENT_TYPE_INVALID                 = iota
	EVENT_TYPE_PARTITION_CREATE
	EVENT_TYPE_PARTITION_DELETE        // partition is in cluster
	EVENT_TYPE_FORCE_PARTITION_DELETE  // partition is not in cluster
)

var (
	processorStarted = false

	partitionProcessor *PartitionProcessor
)

type ProcessorEvent struct {
	typ  int
	body interface{}
}

func NewPartitionCreateEvent(partition *Partition) *ProcessorEvent {
	return &ProcessorEvent{
		typ:  EVENT_TYPE_PARTITION_CREATE,
		body: partition,
	}
}

// internal use
type PartitionDeleteBody struct {
	partitionId     metapb.PartitionID
	leaderNodeId    metapb.NodeID
	replicaRpcAddr  string
	replica         *metapb.Replica
}

func NewPartitionDeleteEvent(partitionId metapb.PartitionID, leaderNodeId metapb.NodeID,
	replica *metapb.Replica) *ProcessorEvent {
	return &ProcessorEvent{
		typ: EVENT_TYPE_PARTITION_DELETE,
		body: &PartitionDeleteBody{
			partitionId:  partitionId,
			leaderNodeId: leaderNodeId,
			replica:      replica,
		},
	}
}

// internal use
func NewForcePartitionDeleteEvent(partitionId metapb.PartitionID, replicaRpcAddr string,
			replica *metapb.Replica) *ProcessorEvent {
	return &ProcessorEvent{
		typ: EVENT_TYPE_FORCE_PARTITION_DELETE,
		body: &PartitionDeleteBody{
			partitionId:     partitionId,
			replicaRpcAddr:  replicaRpcAddr,
			replica:         replica,
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
	psRpcClient    *PSRpcClient
	// psRpcClient    *rpc.Client
}

func (p *PartitionProcessor) doRun() {
	idGenerator := GetIdGeneratorInstance(nil)

	for {
		select {
		case <-p.ctx.Done():
			return
		case event := <-p.eventCh:

			if event.typ == EVENT_TYPE_PARTITION_CREATE {
				psToCreate := p.serverSelector.SelectTarget(p.cluster.psCache.getAllServers())
				if psToCreate == nil {
					log.Error("Can not distribute suitable ps")
					// TODO: calling jdos api to allocate a container asynchronously
					break
				}

				go func(partitionToCreate *Partition) {
					leaderPS := p.cluster.psCache.findServerById(partitionToCreate.pickLeaderNodeId()s)
					// leaderPS is nil when create first partition

					replicaId, err := idGenerator.GenID()
					if err != nil {
						log.Error("fail to generate new replica ßid. err:[%v]", err)
						return
					}
					var newMetaReplica = &metapb.Replica{ID: metapb.ReplicaID(replicaId), NodeID: psToCreate.ID}

					partitionCopy := deepcopy.Iface(partitionToCreate.Partition).(*metapb.Partition)
					partitionCopy.Replicas = append(partitionCopy.Replicas, *newMetaReplica)
					if err := p.psRpcClient.CreatePartition(psToCreate.getRpcAddr(), partitionCopy); err != nil {
						log.Error("Rpc fail to create partition[%v] into ps. err:[%v]",
							partitionToCreate.Partition, err)
						return
					}

					if leaderPS != nil {
						if err := p.psRpcClient.AddReplica(leaderPS.getRpcAddr(), partitionToCreate.ID, &psToCreate.RaftAddrs,
							newMetaReplica.ID, newMetaReplica.NodeID); err != nil {
							log.Error("Rpc fail to add replica[%v] into leader ps. err[%v]", newMetaReplica, err)
							return
						}
					}

				}(event.body.(*Partition))

			} else if event.typ == EVENT_TYPE_PARTITION_DELETE {

				body := event.body.(*PartitionDeleteBody)

				go func(partitionId metapb.PartitionID, leaderNodeId metapb.NodeID, replica *metapb.Replica) {
					leaderPS := p.cluster.psCache.findServerById(leaderNodeId)
					if leaderPS == nil {
						log.Debug("can not find leader ps when notify deleting replicas to leader")
						return
					}
					psToDelete := p.cluster.psCache.findServerById(replica.NodeID)
					if psToDelete == nil {
						log.Debug("can not find replica[%v] ps needed to deleted", replica.NodeID)
						return
					}

					if err := p.psRpcClient.RemoveReplica(leaderPS.getRpcAddr(), partitionId, &psToDelete.RaftAddrs,
						replica.ID, replica.NodeID); err != nil {
						log.Error("Rpc fail to remove replica[%v] from ps. err[%v]", replica.ID, err)
						return
					}

					if err := p.psRpcClient.DeletePartition(psToDelete.getRpcAddr(), partitionId); err != nil {
						log.Error("Rpc fail to delete partition[%v] from ps. err:[%v]", partitionId, err)
						return
					}

				}(body.partitionId, body.leaderNodeId, body.replica)

			} else if event.typ == EVENT_TYPE_FORCE_PARTITION_DELETE {

				body := event.body.(*PartitionDeleteBody)

				go func(partitionId metapb.PartitionID, replicaRpcAddr string, replica *metapb.Replica) {
					if err := p.psRpcClient.DeletePartition(replicaRpcAddr, partitionId); err != nil {
						log.Error("Rpc fail to delete partition[%v] from ps. err:[%v]", partitionId, err)
						return
					}
				}(body.partitionId, body.replicaRpcAddr, body.replica)
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

	//connMgrOpt := rpc.DefaultManagerOption
	//connMgr := rpc.NewConnectionMgr(p.ctx, &connMgrOpt)
	//clientOpt := rpc.DefaultClientOption
	//clientOpt.ClusterID = "1"
	//clientOpt.ConnectMgr = connMgr
	//clientOpt.CreateFunc = func(cc *grpc.ClientConn) interface{} { return pspb.NewAdminGrpcClient(cc) }
	//p.psRpcClient = rpc.NewClient(1, &clientOpt)

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
		log.Error("empty event")
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
