package master

import (
	"proto/metapb"
	"github.com/google/btree"
	"sync"
	"bytes"
)

const (
	PREFIX_PARTITION 	string = "schema partition"
	defaultBTreeDegree = 64
)

type Partition struct {
	*metapb.Partition

	//for splitting & merging
	leftCh  *Partition
	rightCh *Partition
	parent  *Partition

	status string //serving, splitting, cleaning, etc.
}

//type ReplicaGroup struct {
//	id       uint32
//	replicas []*PartitionServer
//}

type PartitionCache struct {
	lock  		 sync.RWMutex
	partitions   map[uint32]*Partition
}

type PartitionItem struct {
	partition   *metapb.Partition
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
	item := &rangeItem{region: rng}

	result := t.find(rng)
	if result == nil {
		result = item
	}

	var overlaps []*rangeItem
	var count int
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		over := i.(*rangeItem)
		if bytes.Compare(rng.EndKey, over.region.StartKey) <= 0 {
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
	if result == nil || result.region.GetId() != rng.GetId() {
		return
	}

	t.tree.Delete(result)
}

// search returns a region that contains the key.
func (t *PartitionTree) search(key []byte) *metapb.Partition {
	rng := &metapb.Range{StartKey: key}
	log.Debug("################### len=%v", t.tree.Len())
	result := t.find(rng)
	if result == nil {
		return nil
	}
	return result.region
}

func (t *PartitionTree) multipleSearch(key []byte, num int) []*metapb.Partition {
	rng := &metapb.Range{StartKey: key}
	results := t.ascendScan(rng, num)
	var ranges []*metapb.Range
	ranges = make([]*metapb.Range, 0, num)
	var endKey []byte
	for _, r := range results {
		if len(endKey) != 0 {
			if bytes.Compare(r.region.GetStartKey(), endKey) != 0 {
				break
			}
		}
		ranges = append(ranges, r.region)
		endKey = r.region.GetEndKey()
	}
	return ranges
}

// This is a helper function to find an item.
func (t *PartitionTree) find(rng *metapb.Partition) *PartitionItem {
	item := &rangeItem{region: rng}

	var result *rangeItem
	t.tree.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*rangeItem)
		return false
	})

	log.Debug("####range find: result=%v, startkey=%v", result, rng.StartKey)

	if result != nil {
		log.Debug("####range find: result range =%v, startkey=%v", result.region, rng.StartKey)
	}

	if result == nil || !result.Contains(rng.StartKey) {
		return nil
	}

	return result
}

func (t *PartitionTree) ascendScan(rng *metapb.Partition, num int) []*PartitionItem {
	result := t.find(rng)
	if result == nil {
		return nil
	}

	var results []*rangeItem
	//var firstItem *rangeItem
	results = make([]*rangeItem, 0, num)
	count := 0
	t.tree.DescendLessOrEqual(result, func(i btree.Item) bool {
		results = append(results, i.(*rangeItem))
		count++
		if count == num {
			return false
		} else {
			return true
		}
	})
	return results
}

