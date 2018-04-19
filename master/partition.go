package master

import (
	"proto/metapb"
	"github.com/google/btree"
	"sync"
	"util/log"
	"util/deepcopy"
	"fmt"
	"time"
	"github.com/gogo/protobuf/proto"
	"util"
)

const (
	PREFIX_PARTITION    	  = "schema partition "
	defaultBTreeDegree        = 64
	FIXED_REPLICA_NUM   	  = 3
)

type Partition struct {
	*metapb.Partition  // !!! 不要直接操作Replicas，要先获取propertyLock

	lastHeartbeat time.Time
	propertyLock  sync.RWMutex
	//	replicaGroup *ReplicaGroup
}

func NewPartition(dbId, spaceId, startSlot, endSlot uint32) (*Partition, error) {
	partId, err := GetIdGeneratorInstance(nil).GenID()
	if err != nil {
		log.Error("generate partition id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}
	return &Partition{
		Partition: &metapb.Partition{
			ID:        partId,
			DB:        dbId,
			Space:     spaceId,
			StartSlot: startSlot,
			EndSlot:   endSlot,
			Replicas:  make([]metapb.Replica, 0),
			Status:    metapb.PartitionStatus_PA_NOTREAD,
		},
	}, nil
}

func NewPartitionByMeta(metaPartition *metapb.Partition) *Partition {
	return &Partition{
		Partition: metaPartition,
	}
}

func (p *Partition) batchPersistent(batch Batch) error {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	copy := deepcopy.Iface(p.Partition).(*metapb.Partition)
	marshalVal, err := proto.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal partition[%v]. err:[%v]", copy, err)
		return err
	}
	key := []byte(fmt.Sprintf("%s%d", PREFIX_PARTITION, copy.ID))
	batch.Put(key, marshalVal)

	return nil
}

func (p *Partition) deleteReplica(metaReplicas ...*metapb.Replica) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	for i := len(p.Replicas) - 1; i >= 0; i-- {
		for _, metaReplica := range metaReplicas {
			if p.Replicas[i].ID == metaReplica.ID {
				p.Replicas = append(p.Replicas[:i], p.Replicas[i+1:]...)
			}
		}
	}
}

func (p *Partition) addReplica(metaReplicas *metapb.Replica) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	p.Replicas = append(p.Replicas, *metaReplicas)
}

func (p *Partition) getAllReplicas() []*metapb.Replica {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	replicas := make([]*metapb.Replica, len(p.Replicas))
	for _, metaReplica := range p.Replicas {
		replicas = append(replicas, &metaReplica)
	}

	return replicas
}

//type ReplicaGroup struct {
//	lock 		sync.RWMutex
//	replicas 	map[uint32]*Replica  // key: replicaId
//	servers  	map[uint32]*PartitionServer // key:replicaId
//}

//func (g *ReplicaGroup) getAllServers() []*PartitionServer {
//	g.lock.RLock()
//	defer g.lock.RUnlock()
//
//	servers := make([]*PartitionServer, 0)
//	for _, server := range g.servers {
//		servers = append(servers, server)
//	}
//	return servers
//}
//
//func (g *ReplicaGroup) addReplica(replica *Replica, server *PartitionServer) {
//	g.lock.Lock()
//	defer g.lock.RUnlock()
//
//	g.replicas[replica.GetId()] = replica
//	g.servers[replica.GetId()] = server
//}
//
//func (g *ReplicaGroup) findReplicaById(replicaId uint32) *Replica {
//	g.lock.RLock()
//	defer g.lock.RUnlock()
//
//	r, ok := g.replicas[replicaId]
//	if !ok {
//		return nil
//	}
//
//	return r
//}
//
//func (g *ReplicaGroup) count() int {
//	g.lock.RLock()
//	defer g.lock.RUnlock()
//
//	return len(g.replicas)
//}

type PartitionCache struct {
	lock  		 sync.RWMutex
	partitions   map[uint32]*Partition
}

func NewPartitionCache() *PartitionCache {
	return &PartitionCache{
		partitions: make(map[uint32]*Partition),
	}
}

func (c *PartitionCache) findPartitionById(partitionId uint32) *Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	p, ok := c.partitions[partitionId]
	if !ok {
		return nil
	}

	return p
}

func (c *PartitionCache) addPartition(partition *Partition) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.partitions[partition.ID] = partition
}

func (c *PartitionCache) getAllMetaPartitions() []*metapb.Partition {
	c.lock.RLock()
	defer c.lock.RUnlock()

	partitions := make([]*metapb.Partition, len(c.partitions))
	for _, metaPartition := range c.partitions {
		partitions = append(partitions, metaPartition.Partition)
	}

	return partitions
}


func (c *PartitionCache) recovery(store Store) ([]*Partition, error) {
	prefix := []byte(PREFIX_PARTITION)
	startKey, limitKey := util.BytesPrefix(prefix)

	resultPartitions := make([]*Partition, 0)

	iterator := store.Scan(startKey, limitKey)
	defer iterator.Release()
	for iterator.Next() {
		if iterator.Key() == nil {
			log.Error("partition store key is nil. never happened!!!")
			continue
		}

		val := iterator.Value()
		metaPartition := new(metapb.Partition)
		if err := proto.Unmarshal(val, metaPartition); err != nil {
			log.Error("fail to unmarshal partition from store. err[%v]", err)
			return nil, ErrInternalError
		}

		resultPartitions = append(resultPartitions, NewPartitionByMeta(metaPartition))
	}

	return resultPartitions, nil
}

type PartitionItem struct {
	partition   *metapb.Partition
}

// Less returns true if the region start key is greater than the other.
// So we will sort the region with start key reversely.
func (r *PartitionItem) Less(other btree.Item) bool {
	left := r.partition.StartSlot
	right := other.(*PartitionItem).partition.StartSlot
	//return bytes.Compare(left, right) > 0
	return left > right
}

func (r *PartitionItem) Contains(slot uint32) bool {
	start, end := r.partition.StartSlot, r.partition.EndSlot
	//return bytes.Compare(key, start) >= 0 && bytes.Compare(key, end) < 0
	return slot >= start && slot < end
}

type PartitionTree struct {
	tree 	*btree.BTree
}

func NewPartitionTree() *PartitionTree {
	return &PartitionTree{
		tree: btree.New(defaultBTreeDegree),
	}
}

func (t *PartitionTree) length() int {
	return t.tree.Len()
}

// update updates the tree with the region.
// It finds and deletes all the overlapped regions first, and then
// insert the region.
func (t *PartitionTree) update(rng *metapb.Partition) {
	item := &PartitionItem{partition: rng}

	result := t.find(rng)
	if result == nil {
		result = item
	}

	var overlaps []*PartitionItem
	var count int
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*PartitionItem)
		//if bytes.Compare(rng.EndSlot, over.region.StartKey) <= 0 {
		if rng.EndSlot <= over.partition.StartSlot {
			return false
		}
		overlaps = append(overlaps, over)
		count++
		return true
	})

	if count > 2 {
		log.Warn("=========many overlaps ranges %v, new range[%v]", overlaps, rng)
	}
	for _, item := range overlaps {
		t.tree.Delete(item)
	}

	t.tree.ReplaceOrInsert(item)
}

// remove removes a region if the region is in the tree.
// It will do nothing if it cannot find the region or the found region
// is not the same with the region.
func (t *PartitionTree) remove(rng *metapb.Partition) {
	result := t.find(rng)
	if result == nil || result.partition.ID != rng.ID {
		return
	}

	t.tree.Delete(result)
}

// search returns a region that contains the key.
func (t *PartitionTree) search(slot uint32) *metapb.Partition {
	rng := &metapb.Partition{StartSlot: slot}
	log.Debug("################### len=%v", t.tree.Len())
	result := t.find(rng)
	if result == nil {
		return nil
	}
	return result.partition
}

func (t *PartitionTree) multipleSearch(slot uint32, num int) []*metapb.Partition {
	rng := &metapb.Partition{StartSlot: slot}
	results := t.ascendScan(rng, num)
	var ranges []*metapb.Partition
	ranges = make([]*metapb.Partition, 0, num)
	var endSlot uint32
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
func (t *PartitionTree) find(rng *metapb.Partition) *PartitionItem {
	item := &PartitionItem{partition: rng}

	var result *PartitionItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*PartitionItem)
		return false
	})

	log.Debug("####range find: result=%v, startkey=%v", result, rng.StartSlot)

	if result != nil {
		log.Debug("####range find: result range =%v, startkey=%v", result.partition, rng.StartSlot)
	}

	if result == nil || !result.Contains(rng.StartSlot) {
		return nil
	}

	return result
}

func (t *PartitionTree) ascendScan(rng *metapb.Partition, num int) []*PartitionItem {
	result := t.find(rng)
	if result == nil {
		return nil
	}

	var results []*PartitionItem
	//var firstItem *rangeItem
	results = make([]*PartitionItem, 0, num)
	count := 0
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
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