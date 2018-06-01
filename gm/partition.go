package gm

import (
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/deepcopy"
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

	ReplicaLeader *metapb.Replica
	Term          uint64
	Zones         map[string]*topo.ZoneTopo

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

func (p *Partition) update() error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	err := topoServer.UpdatePartition(ctx, p.PartitionTopo)
	if err != nil {
		log.Error("topoServer UpdatePartition error, err: [%v]", err)
		return err
	}

	return nil
}

//GetZonesForPartition(ctx context.Context, partitionId metapb.PartitionID) ([]string, error)

func (p *Partition) setZones(zonesName []string) error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	err := topoServer.SetZonesForPartition(ctx, p.ID, zonesName)
	if err != nil {
		log.Error("topoServer SetZonesForPartition error, err: [%v]", err)
		return err
	}

	return nil
}

func (p *Partition) getZones() ([]string, error) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zonesName, err := topoServer.GetZonesForPartition(ctx, p.ID)
	if err != nil {
		log.Error("topoServer GetZonesForPartition error, err: [%v]", err)
		return nil, err
	}

	return zonesName, nil
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

func (p *Partition) updateReplicaGroup(partitionInfo *masterpb.PartitionInfo) (bool, error) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	if !partitionInfo.IsLeader {
		return false, nil
	}
	if partitionInfo.Epoch.ConfVersion < p.Epoch.ConfVersion {
		return false, nil
	}
	if partitionInfo.Epoch.ConfVersion == p.Epoch.ConfVersion {
		if p.ReplicaLeader != nil && partitionInfo.RaftStatus.Replica.ID == p.ReplicaLeader.ID {
			return false, nil
		}
		if p.ReplicaLeader != nil && partitionInfo.RaftStatus.Replica.ID != p.ReplicaLeader.ID {
			if partitionInfo.RaftStatus.Term <= p.Term {
				return false, nil
			}
		}
	}

	zonesNameMap := make(map[string]string)
	zonesName := make([]string, 0)
	PartitionTopoCopy := deepcopy.Iface(p.PartitionTopo).(*topo.PartitionTopo)
	PartitionTopoCopy.Replicas = make([]metapb.Replica, 0, len(partitionInfo.RaftStatus.Followers)+1)
	PartitionTopoCopy.Epoch = partitionInfo.Epoch
	PartitionTopoCopy.Status = partitionInfo.Status

	PartitionTopoCopy.Replicas = append(PartitionTopoCopy.Replicas, partitionInfo.RaftStatus.Replica)
	zonesNameMap[partitionInfo.RaftStatus.Replica.Zone] = partitionInfo.RaftStatus.Replica.Zone
	for _, follower := range partitionInfo.RaftStatus.Followers {
		PartitionTopoCopy.Replicas = append(PartitionTopoCopy.Replicas, follower.Replica)
		zonesNameMap[follower.Replica.Zone] = follower.Replica.Zone
	}

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	err := topoServer.UpdatePartition(ctx, PartitionTopoCopy)
	if err != nil {
		log.Error("topoServer UpdatePartition error, err: [%v]", err)
		return false, err
	}
	p.PartitionTopo = PartitionTopoCopy
	p.ReplicaLeader = &partitionInfo.RaftStatus.Replica
	p.Term = partitionInfo.RaftStatus.Term

	for _, zoneNameMap := range zonesNameMap {
		zonesName = append(zonesName, zoneNameMap)
	}
	err = p.setZones(zonesName)
	if err != nil {
		log.Error("setZones error, err: [%v]", err)
		return false, err
	}

	return true, nil
}

func (p *Partition) pickReplicaToDelete() *metapb.Replica {
	var replicaToDelete *metapb.Replica

	if p.Replicas == nil || len(p.Replicas) == 0 {
		return nil
	}
	if len(p.Replicas) == 1 {
		return &p.Replicas[0]
	}
	for _, replica := range p.Replicas {
		if p.ReplicaLeader != nil && replica.ID == p.ReplicaLeader.ID {
			continue
		}
		replicaToDelete = &replica
		break
	}
	return replicaToDelete
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
