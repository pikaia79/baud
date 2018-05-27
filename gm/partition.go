package gm

import (
	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"golang.org/x/net/context"
)

const (
	defaultBTreeDegree = 64
	FIXED_REPLICA_NUM  = 1
)

type Partition struct {
	*topo.PartitionTopo

    Zones    map[string]*topo.ZoneTopo

	propertyLock  sync.RWMutex
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

func (p *Partition) pickLeaderNodeId() metapb.NodeID {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	// TODO 得到一个partition的replica leader
	// TODO 需要watch所有zone的etcd后,才能分析出来

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

func (p *Partition) takeChangeMemberTask() bool {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	// TODO 得到一个partition的任务标识， 接口由@杨洋提供

	return false
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

	ctx := context.Background()
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

// PartitionItem

type PartitionItem struct {
	partition *Partition
}

// Less returns true if the region start key is greater than the other.
// So we will sort the region with start key reversely.
func (r *PartitionItem) Less(other btree.Item) bool {
	left := r.partition.StartSlot
	right := other.(*PartitionItem).partition.StartSlot
	//return bytes.Compare(left, right) > 0
	return left > right
}

func (r *PartitionItem) Contains(slot metapb.SlotID) bool {
	start, end := r.partition.StartSlot, r.partition.EndSlot
	//return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
	return slot >= start && slot < end
}

type PartitionTree struct {
	tree *btree.BTree
}

func NewPartitionTree() *PartitionTree {
	return &PartitionTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

func (pt *PartitionTree) length() int {
	return pt.tree.Len()
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (pt *PartitionTree) update(partition *Partition) {
	item := &PartitionItem{partition: partition}

	result := pt.find(partition)
	if result == nil {
		result = item
	}

	var overlaps []*PartitionItem
	var count int
	pt.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*PartitionItem)
		//if bytes.Compare(rng.EndSlot, over.region.StartKey) <= 0 {
		if partition.EndSlot <= over.partition.StartSlot {
			return false
		}
		overlaps = append(overlaps, over)
		count++
		return true
	})

	if count > 2 {
		log.Warn("=========many overlaps ranges %v, new range[%v]", overlaps, partition)
	}
	for _, o := range overlaps {
		pt.tree.Delete(o)
	}

	pt.tree.ReplaceOrInsert(item)
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (pt *PartitionTree) remove(partition *Partition) {
	result := pt.find(partition)
	if result == nil || result.partition.ID != partition.ID {
		return
	}

	pt.tree.Delete(result)
}

// search returns a region that contains the key.
func (pt *PartitionTree) search(slot metapb.SlotID) *Partition {
	rng := &Partition{
		Partition: &metapb.Partition{
			StartSlot: slot,
		},
	}
	log.Debug("################### len=%v", pt.tree.Len())
	result := pt.find(rng)
	if result == nil {
		return nil
	}
	return result.partition
}

func (pt *PartitionTree) multipleSearch(slot metapb.SlotID, num int) []*Partition {
	rng := &Partition{
		Partition: &metapb.Partition{
			StartSlot: slot,
		},
	}
	results := pt.ascendScan(rng, num)
	var ranges = make([]*Partition, 0, num)
	var endSlot metapb.SlotID
	var isFound = false
	for _, r := range results {
		//if len(endKey) != 0 {
		if isFound {
			//if bytes.Compare(r.region.GetStartKey(), endKey) != 0 {
			if r.partition.StartSlot != endSlot {
				break
			}
		}
		ranges = append(ranges, r.partition)
		endSlot = r.partition.EndSlot
		isFound = true
	}
	return ranges
}

// This is a helper function to find an item.
func (pt *PartitionTree) find(partition *Partition) *PartitionItem {
	item := &PartitionItem{partition: partition}

	var result *PartitionItem
	pt.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*PartitionItem)
		return false
	})

	log.Debug("####range find: result=%v, startkey=%v", result, partition.StartSlot)

	if result != nil {
		log.Debug("####range find: result range =%v, startkey=%v", result.partition, partition.StartSlot)
	}

	if result == nil || !result.Contains(partition.StartSlot) {
		return nil
	}

	return result
}

func (pt *PartitionTree) ascendScan(partition *Partition, num int) []*PartitionItem {
	result := pt.find(partition)
	if result == nil {
		return nil
	}

	var results []*PartitionItem
	//var firstItem *rangeItem
	results = make([]*PartitionItem, 0, num)
	count := 0
	pt.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		results = append(results, i.(*PartitionItem))
		count++
		if count == num {
			return false
		} else {
			return true
		}
	})
	return results
}
