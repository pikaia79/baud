package gm

import (
	"context"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/deepcopy"
	"github.com/tiglabs/baudengine/util/log"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

const (
	PARTITION_CHANNEL_LIMIT = 1000
)

const (
	EVENT_TYPE_INVALID = iota
	EVENT_TYPE_PARTITION_CREATE
	EVENT_TYPE_PARTITION_DELETE       // partition is in cluster
	EVENT_TYPE_FORCE_PARTITION_DELETE // partition is not in cluster
)

var (
	processorManagerSingle     *ProcessorManager
	processorManagerSingleLock sync.Mutex
	processorManagerSingleDone uint32
)

type ProcessorManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	pp *PartitionProcessor

	isStarted bool
	wg        sync.WaitGroup
}

func GetPMSingle(cluster *Cluster) *ProcessorManager {
	if processorManagerSingle != nil {
		return processorManagerSingle
	}
	if atomic.LoadUint32(&processorManagerSingleDone) == 1 {
		return processorManagerSingle
	}

	processorManagerSingleLock.Lock()
	defer processorManagerSingleLock.Unlock()

	if atomic.LoadUint32(&processorManagerSingleDone) == 0 {
		if cluster == nil {
			log.Error("cluster should not be nil at first time when create ProcessorManager single")
		}

		pm := new(ProcessorManager)
		pm.ctx, pm.cancel = context.WithCancel(context.Background())
		pm.pp = NewPartitionProcessor(pm.ctx, pm.cancel, cluster)

		processorManagerSingle = pm
		atomic.StoreUint32(&processorManagerSingleDone, 1)

		log.Info("ProcessorManager single has started")
	}

	return processorManagerSingle
}

func (pm *ProcessorManager) Close() {
	if !pm.isStarted {
		return
	}
	pm.isStarted = false

	pm.cancel()
	pm.wg.Wait()

	pm.pp.Close()

	processorManagerSingleLock.Lock()
	defer processorManagerSingleLock.Unlock()

	processorManagerSingle = nil
	atomic.StoreUint32(&processorManagerSingleDone, 0)

	log.Info("ProcessorManager single has closed")
}

func (pm *ProcessorManager) Start() {
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()
		defer func() {
			if e := recover(); e != nil {
				log.Error("recover partition processor panic. e:[%s]\n stack:[%s]", e, debug.Stack())
			}
		}()

		pm.pp.Run()
	}()

	pm.isStarted = true
	log.Info("Processor manager has started")
}

func (pm *ProcessorManager) PushEvent(event *ProcessorEvent) error {
	if event == nil {
		log.Error("empty event")
		return ErrInternalError
	}

	if !pm.isStarted {
		log.Error("processor manager is not started")
		return ErrInternalError
	}

	if event.typ == EVENT_TYPE_PARTITION_CREATE ||
		event.typ == EVENT_TYPE_PARTITION_DELETE ||
		event.typ == EVENT_TYPE_FORCE_PARTITION_DELETE {

		if len(pm.pp.eventCh) >= PARTITION_CHANNEL_LIMIT*0.9 {
			log.Error("partition channel will full, reject event[%v]", event)
			return ErrSysBusy
		}

		pm.pp.eventCh <- event

	} else {
		log.Error("processor received invalid event type[%v]", event.typ)
		return ErrInternalError
	}

	return nil
}

type ProcessorEvent struct {
	typ  int
	body interface{}
}

type PartitionCreateBody struct {
	replicaZMAddr       string
	replicaLeaderZMAddr string
	partition           *Partition
}

type PartitionDeleteBody struct {
	replicaZMAddr       string
	replicaLeaderZMAddr string
	partitionId         metapb.PartitionID
	replica             *metapb.Replica
}

type PartitionForceDeleteBody struct {
	replicaZMAddr string
	partitionId   metapb.PartitionID
}

func NewPartitionCreateEvent(
	replicaZMAddr string,
	replicaLeaderZMAddr string,
	partition *Partition) *ProcessorEvent {
	return &ProcessorEvent{
		typ: EVENT_TYPE_PARTITION_CREATE,
		body: &PartitionCreateBody{
			replicaZMAddr:       replicaZMAddr,
			replicaLeaderZMAddr: replicaLeaderZMAddr,
			partition:           partition,
		},
	}
}

func NewPartitionDeleteEvent(
	replicaZMAddr string,
	replicaLeaderZMAddr string,
	partitionId metapb.PartitionID,
	replica *metapb.Replica) *ProcessorEvent {
	return &ProcessorEvent{
		typ: EVENT_TYPE_PARTITION_DELETE,
		body: &PartitionDeleteBody{
			replicaZMAddr:       replicaZMAddr,
			replicaLeaderZMAddr: replicaLeaderZMAddr,
			partitionId:         partitionId,
			replica:             replica,
		},
	}
}

func NewPartitionForceDeleteEvent(
	replicaZMAddr string,
	partitionId metapb.PartitionID) *ProcessorEvent {
	return &ProcessorEvent{
		typ: EVENT_TYPE_FORCE_PARTITION_DELETE,
		body: &PartitionDeleteBody{
			replicaZMAddr: replicaZMAddr,
			partitionId:   partitionId,
		},
	}
}

type Processor interface {
	Run()
	Close()
}

type PartitionProcessor struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	eventCh chan *ProcessorEvent
	cluster *Cluster
}

func NewPartitionProcessor(ctx context.Context, cancel context.CancelFunc, cluster *Cluster) *PartitionProcessor {
	pp := &PartitionProcessor{
		ctx:        ctx,
		cancelFunc: cancel,
		eventCh:    make(chan *ProcessorEvent, PARTITION_CHANNEL_LIMIT),
		cluster:    cluster,
	}
	return pp
}

func (pp *PartitionProcessor) Run() {
	log.Info("Partition Processor is running")

	for {
		select {
		case <-pp.ctx.Done():
			log.Info("Partition Processor exit")
			return
		case event, ok := <-pp.eventCh:
			if !ok {
				log.Debug("closed partition processor event channel")
				return
			}
			pp.wg.Add(1)
			if event.typ == EVENT_TYPE_PARTITION_CREATE {
				go func() {
					defer pp.wg.Done()
					body := event.body.(*PartitionCreateBody)
					log.Debug("EVENT_TYPE_PARTITION_CREATE replicaZMAddr: [%s], replicaLeaderZMAddr:[%s], partition:[%v]", body.replicaZMAddr, body.replicaLeaderZMAddr, body.partition)
					pp.createPartition(body.replicaZMAddr, body.replicaLeaderZMAddr, body.partition)
				}()
			} else if event.typ == EVENT_TYPE_PARTITION_DELETE {
				go func() {
					defer pp.wg.Done()
					body := event.body.(*PartitionDeleteBody)
					log.Debug("EVENT_TYPE_PARTITION_DELETE replicaZMAddr: [%s], replicaLeaderZMAddr:[%s], partitionId:[%d], replica:[%v]", body.replicaZMAddr, body.replicaLeaderZMAddr, body.partitionId, body.replica)
					pp.deletePartition(body.replicaZMAddr, body.replicaLeaderZMAddr, body.partitionId, body.replica)
				}()
			} else if event.typ == EVENT_TYPE_FORCE_PARTITION_DELETE {
				go func() {
					defer pp.wg.Done()
					body := event.body.(*PartitionDeleteBody)
					log.Debug("EVENT_TYPE_FORCE_PARTITION_DELETE replicaZMAddr: [%s], replicaLeaderZMAddr:[%s], partitionId:[%d], replica:[%v]", body.replicaZMAddr, body.replicaLeaderZMAddr, body.partitionId, body.replica)
					pp.forceDeletePartition(body.replicaZMAddr, body.partitionId)
				}()
			}
		}
	}
}

func (pp *PartitionProcessor) Close() {
	if pp.eventCh != nil {
		close(pp.eventCh)
	}
	pp.wg.Wait()
}

func (pp *PartitionProcessor) createPartition(replicaZMAddr, replicaLeaderZMAddr string, partition *Partition) {
	replicaId, err := GetIdGeneratorSingle().GenID()
	if err != nil {
		log.Error("fail to generate new replica id. err:[%v]", err)
		return
	}
	newMetaReplica := &metapb.Replica{
		ID: metapb.ReplicaID(replicaId),
	}

	partitionCopy := deepcopy.Iface(partition).(*metapb.Partition)
	partitionCopy.Replicas = append(partitionCopy.Replicas, *newMetaReplica)
	replicaMetaResp, err := GetZoneMasterRpcClientSingle(pp.cluster.gm.config).CreatePartition(replicaZMAddr, partitionCopy)
	if err != nil {
		log.Error("Rpc fail to create partition[%v] in replicaZMAddr:[%s]. err:[%v]", partitionCopy, replicaZMAddr, err)
		return
	}

	err = GetZoneMasterRpcClientSingle(pp.cluster.gm.config).AddReplica(replicaLeaderZMAddr, partitionCopy.ID, replicaMetaResp)
	if err != nil {
		log.Error("Rpc fail to add replica[%v] into partition[%v] in replicaLeaderZMAddr:[%s]. err[%v]", replicaMetaResp, replicaLeaderZMAddr, partitionCopy, err)
		return
	}
}

func (pp *PartitionProcessor) deletePartition(replicaZMAddr, replicaLeaderZMAddr string, partitionId metapb.PartitionID, replica *metapb.Replica) {
	if err := GetZoneMasterRpcClientSingle(pp.cluster.gm.config).RemoveReplica(replicaLeaderZMAddr, partitionId, replica); err != nil {
		log.Error("Rpc fail to remove replica[%v] from partitionId:[%d] in replicaLeaderZMAddr:[%s]. err[%v]", replica, partitionId, replicaLeaderZMAddr, err)
		return
	}

	if err := GetZoneMasterRpcClientSingle(pp.cluster.gm.config).DeletePartition(replicaZMAddr, partitionId); err != nil {
		log.Error("Rpc fail to delete partition[%s] in replicaZMAddr:[%s]. err:[%v]", partitionId, err)
		return
	}
}

func (pp *PartitionProcessor) forceDeletePartition(replicaRpcAddr string, partitionId metapb.PartitionID) {
	if err := GetZoneMasterRpcClientSingle(pp.cluster.gm.config).DeletePartition(replicaRpcAddr, partitionId); err != nil {
		log.Error("Rpc fail to delete partition[%s] in replicaZMAddr:[%s]. err:[%v]", partitionId, err)
		return
	}
}
