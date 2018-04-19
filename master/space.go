package master

import (
	"sync"
	"proto/metapb"
	"util/log"
	"util/deepcopy"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"util"
)

const (
	PREFIX_SPACE   = "schema space "
)

type PartitionPolicy struct {
	Key           string
	Function      string
	NumPartitions int
}

type Space struct {
	*metapb.Space

	// TODO:move partitioning to metapb.Space definition
	partitioning *PartitionPolicy    `json:"-"`
	mapping      []Field

	searchTree   *PartitionTree      `json:"-"`
	propertyLock sync.RWMutex        `json:"-"`
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}

func NewSpace(dbId uint32, dbName, spaceName string, policy *PartitionPolicy) (*Space, error) {
	spaceId, err := GetIdGeneratorInstance(nil).GenID()
	if err != nil {
		log.Error("generate space id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}

	return &Space{
		Space: &metapb.Space{
			Name:   spaceName,
			ID:     spaceId,
			DB:   	dbId,
			DbName: dbName,
			Status: metapb.SS_Init,
		},
		partitioning: policy,
		searchTree: NewPartitionTree(),
	}, nil
}

func NewSpaceByMeta(metaSpace *metapb.Space) *Space {
	return &Space{
		Space:      metaSpace,
		searchTree: NewPartitionTree(),
	}
}

func (s *Space) persistent(store Store) error {
	s.propertyLock.RLock()
	defer s.propertyLock.RUnlock()

	copy := deepcopy.Iface(s.Space).(*metapb.Space)
	spaceVal, err := proto.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal space[%v]. err:[%v]", copy, err)
		return err
	}
	spaceKey := []byte(fmt.Sprintf("%s%d", PREFIX_SPACE, copy.ID))
	if err := store.Put(spaceKey, spaceVal); err != nil {
		log.Error("fail to put space[%v] into store. err:[%v]", copy, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (s *Space) batchPersistent(batch Batch) error {
	s.propertyLock.RLock()
	defer s.propertyLock.RUnlock()

	copy := deepcopy.Iface(s.Space).(*metapb.Space)
	spaceVal, err := proto.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal space[%v]. err:[%v]", copy, err)
		return err
	}
	spaceKey := []byte(fmt.Sprintf("%s%d", PREFIX_SPACE, copy.ID))
	batch.Put(spaceKey, spaceVal)

	return nil
}

func (s *Space) erase(store Store) error {
	s.propertyLock.RLock()
	defer s.propertyLock.RUnlock()

	spaceKey := []byte(fmt.Sprintf("%s%d", PREFIX_SPACE, s.ID))
	if err := store.Delete(spaceKey); err != nil {
		log.Error("fail to delete space[%v] from store. err:[%v]", s.Space, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (s *Space) rename(newName string) {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	s.Name = newName
}

func (s *Space) putPartition(partition *Partition) {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	s.searchTree.update(partition.Partition)
}

type SpaceCache struct {
	lock     sync.RWMutex
	name2Ids map[string]uint32
	spaces   map[uint32]*Space
}

func NewSpaceCache() *SpaceCache {
	return &SpaceCache{
		name2Ids: make(map[string]uint32),
		spaces:   make(map[uint32]*Space),
	}
}

func (c *SpaceCache) findSpaceByName(spaceName string) *Space {
	c.lock.RLock()
	defer c.lock.RUnlock()

	spaceId, ok := c.name2Ids[spaceName]
	if !ok {
		return nil
	}
	space, ok := c.spaces[spaceId]
	if !ok {
		log.Error("!!!space cache map not consistent, space[%v : %v] not exists. never happened", spaceName, spaceId)
		return nil
	}
	return space
}

func (c *SpaceCache) findSpaceById(spaceId uint32) *Space {
	c.lock.RLock()
	defer c.lock.RUnlock()

	space, ok := c.spaces[spaceId]
	if !ok {
		return nil
	}
	return space
}

func (c *SpaceCache) addSpace(space *Space) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.name2Ids[space.Name] = space.ID
	c.spaces[space.ID] = space
}

func (c *SpaceCache) deleteSpace(space *Space) {
	c.lock.Lock()
	defer c.lock.Unlock()

	oldSpace, ok := c.spaces[space.ID]
	if !ok {
		return
	}
	delete(c.name2Ids, oldSpace.Name)
}

func (c *SpaceCache) recovery(store Store) ([]*Space, error) {
	prefix := []byte(PREFIX_SPACE)
	startKey, limitKey := util.BytesPrefix(prefix)

	resultSpaces := make([]*Space, 0)

	iterator := store.Scan(startKey, limitKey)
	defer iterator.Release()
	for iterator.Next() {
		if iterator.Key() == nil {
			log.Error("space store key is nil. never happened!!!")
			continue
		}

		val := iterator.Value()
		metaSpace := new(metapb.Space)
		if err := proto.Unmarshal(val, metaSpace); err != nil {
			log.Error("fail to unmarshal space from store. err[%v]", err)
			return nil, ErrInternalError
		}

		resultSpaces = append(resultSpaces, NewSpaceByMeta(metaSpace))
	}

	return resultSpaces, nil
}
