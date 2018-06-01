package gm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"sync"
	"time"
)

const (
	FIXED_REPLICA_NUM = 3
)

type Partition struct {
	*topo.PartitionTopo

	Zones map[string]*topo.ZoneTopo

	propertyLock sync.RWMutex
}

func NewPartition(dbId metapb.DBID, spaceId metapb.SpaceID, startSlot, endSlot metapb.SlotID) (*Partition, error) {
	partId, err := GetIdGeneratorSingle().GenID()
	if err != nil {
		log.Error("generate partition id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}

	partitionMeta := &metapb.Partition{
		ID:        metapb.PartitionID(partId),
		DB:        dbId,
		Space:     spaceId,
		StartSlot: metapb.SlotID(startSlot),
		EndSlot:   metapb.SlotID(endSlot),
		Replicas:  make([]metapb.Replica, 0),
		Status:    metapb.PA_READONLY,
	}

	partitionTopo := &topo.PartitionTopo{
		Partition: partitionMeta,
	}

	return NewPartitionByTopo(partitionTopo), nil
}

func NewPartitionByTopo(partitionTopo *topo.PartitionTopo) *Partition {
	return &Partition{
		PartitionTopo: partitionTopo,
	}
}

func (p *Partition) countReplicas() int {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	return len(p.Replicas)
}

func (p *Partition) getAllReplicas() []*metapb.Replica {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	replicas := make([]*metapb.Replica, 0, len(p.Replicas))
	for _, metaReplica := range p.Replicas {
		replicas = append(replicas, &metaReplica)
	}

	return replicas
}

func (p *Partition) findReplicaById(replicaId metapb.ReplicaID) *metapb.Replica {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	for _, replica := range p.Replicas {
		if replica.ID == replicaId {
			return &replica
		}
	}

	return nil
}

func (p *Partition) grabPartitionTaskLock(zoneName, taskType, taskId string) (bool, error) {
	task, err := p.getPartitionTask(zoneName, taskType, taskId)
	if err != nil {
		return false, err
	}
	if task != nil {
		return false, nil
	}
	task = &metapb.Task{
		Id:   taskId,
		Type: taskType,
	}
	err = p.setPartitionTask(zoneName, task, 30*time.Second)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (p *Partition) getPartitionTask(zoneName, taskType, taskId string) (*metapb.Task, error) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()
	taskMeta, err := topoServer.GetTask(ctx, zoneName, taskType, taskId)
	if err != nil {
		log.Error("topoServer GetTask error, err: [%v]", err)
		return nil, err
	}

	return taskMeta, nil
}

func (p *Partition) setPartitionTask(zoneName string, task *metapb.Task, timeout time.Duration) error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	err := topoServer.AddTask(ctx, zoneName, task, timeout)
	if err != nil {
		log.Error("topoServer SetTask error, err: [%v]", err)
		return err
	}
	return nil
}

// PartitionCache
type PartitionCache struct {
	lock       sync.RWMutex
	partitions map[metapb.PartitionID]*Partition
}

func NewPartitionCache() *PartitionCache {
	return &PartitionCache{
		partitions: make(map[metapb.PartitionID]*Partition),
	}
}

func (c *PartitionCache) FindPartitionById(partitionId metapb.PartitionID) *Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	p, ok := c.partitions[partitionId]
	if !ok {
		return nil
	}

	return p
}

func (c *PartitionCache) AddPartition(partition *Partition) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.partitions[partition.ID] = partition
}

func (c *PartitionCache) DeletePartition(partitionID metapb.PartitionID) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.partitions, partitionID)
}

func (c *PartitionCache) GetAllPartitions() []*Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	partitions := make([]*Partition, 0, len(c.partitions))
	for _, partition := range c.partitions {
		log.Debug("api get all partition[%p] ", partition)
		partitions = append(partitions, partition)
	}

	return partitions
}

func (c *PartitionCache) GetAllMetaPartitions() []*metapb.Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	partitionsMeta := make([]*metapb.Partition, 0, len(c.partitions))
	for _, partition := range c.partitions {
		partitionsMeta = append(partitionsMeta, partition.PartitionTopo.Partition)
	}

	return partitionsMeta
}

func (c *PartitionCache) Recovery() ([]*Partition, error) {

	resultPartitions := make([]*Partition, 0)

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()
	partitionsTopo, err := topoServer.GetAllPartitions(ctx)
	if err != nil {
		log.Error("topoServer GetAllPartitions error, err: [%v]", err)
		return nil, err
	}
	if partitionsTopo != nil {
		for _, partitionTopo := range partitionsTopo {
			partition := &Partition{
				PartitionTopo: partitionTopo,
			}
			resultPartitions = append(resultPartitions, partition)
		}
	}

	return resultPartitions, nil
}

func (c *PartitionCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.partitions = make(map[metapb.PartitionID]*Partition)
}
