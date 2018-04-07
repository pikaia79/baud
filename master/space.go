package master

import (
	"sync"
	"proto/metapb"
	"btree"
	"util/log"
	"util/deepcopy"
	"encoding/json"
	"fmt"
)

const (
	PREFIX_SPACE 	string = "schema space"
)

type PartitionPolicy struct {
	key           string
	function      string
	numPartitions int
}

type Space struct {
	*metapb.Space
	entityOrEdge string

	partitioning PartitionPolicy
	mapping      []Field

	partitions	 btree.BTree        `json:"-"`
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}

func NewSpace(spaceName, partitionKey, partitionFunction string, partitionNum int) (*Space, error) {
	spaceId, err := IdGeneratorSingleInstance(nil).GenID()
	if err != nil {
		log.Error("generate space id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}

	return &Space{
		Space: &metapb.Space{
			Name: spaceName,
			Id:   spaceId,
		},
		partitioning: PartitionPolicy{
			key:           partitionKey,
			function:      partitionFunction,
			numPartitions: partitionNum,
		},
	}, nil
}

func (s *Space) persistent(store Store) error {
	copy := deepcopy.Iface(s.Space).(*metapb.Space)
	spaceVal, err := json.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal space[%v]. err:[%v]", copy, err)
		return err
	}
	spaceKey := []byte(fmt.Sprintf("%s %d", PREFIX_SPACE, copy.Id))
	if err := store.Put(spaceKey, spaceVal); err != nil {
		log.Error("fail to put space[%v] into store. err:[%v]", copy, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (s *Space) erase(store Store) error {
	spaceKey := []byte(fmt.Sprintf("%s %d", PREFIX_SPACE, ))
	if err := store.Delete(spaceKey); err != nil {
		log.Error("fail to delete space[%v] from store. err:[%v]", s.Space, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (s *Space) rename(newName string) {
	s.Name = newName
}

type SpaceCache struct {
	lock 		 sync.RWMutex
	spaceNameMap map[string]uint32
	spaces       map[uint32]*Space
}

func (c *SpaceCache) findSpaceByName(spaceName string) *Space {
	spaceId, ok := c.spaceNameMap[spaceName]
	if !ok {
		return nil
	}
	space, ok := c.spaces[spaceId]
	if !ok {
		log.Error("!!!space cache map not consistent, space[%v] not exists.", spaceId)
		return nil
	}
	return space
}

func (c *SpaceCache) addSpace(space *Space)  {
	c.spaceNameMap[space.Name] = space.Id
	c.spaces[space.Id] = space
}

func (c *SpaceCache) deleteSpace(space *Space) {
	oldSpace, ok := c.spaces[space.Id]
	if !ok {
		return
	}
	delete(c.spaceNameMap, oldSpace.Name)
}
