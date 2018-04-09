package master

import (
	"sync"
	"proto/metapb"
	"util/log"
	"util/deepcopy"
	"encoding/json"
	"fmt"
)

const (
	PREFIX_SPACE 	string = "schema space"
)

type PartitionPolicy struct {
	Key           string
	Function      string
	NumPartitions int
}

type Space struct {
	*metapb.Space

	Partitioning PartitionPolicy
	Mapping      []Field

	Partitions   *PartitionTree      `json:"-"`
	propertyLock sync.RWMutex        `json:"-"`
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}

func NewSpace(dbId uint32, dbName, spaceName, partitionKey, partitionFunction string, partitionNum int) (*Space, error) {
	spaceId, err := IdGeneratorSingleInstance(nil).GenID()
	if err != nil {
		log.Error("generate space id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}

	return &Space{
		Space: &metapb.Space{
			Name:   spaceName,
			Id:     spaceId,
			DbId:   dbId,
			DbName: dbName,
			Status: metapb.SpaceStatus_Init,
		},
		Partitioning: PartitionPolicy{
			Key:           partitionKey,
			Function:      partitionFunction,
			NumPartitions: partitionNum,
		},
	}, nil
}

func (s *Space) persistent(store Store) error {
	s.propertyLock.RLock()
	defer s.propertyLock.RUnlock()

	copy := deepcopy.Iface(*s).(Space)
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
	s.propertyLock.RLock()
	defer s.propertyLock.RUnlock()

	spaceKey := []byte(fmt.Sprintf("%s %d", PREFIX_SPACE, ))
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

type SpaceCache struct {
	lock      sync.RWMutex
	NameIdMap map[string]uint32
	spaces    map[uint32]*Space
}

func (c *SpaceCache) findSpaceByName(spaceName string) *Space {
	c.lock.RLock()
	defer c.lock.RUnlock()

	spaceId, ok := c.NameIdMap[spaceName]
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

func (c *SpaceCache) addSpace(space *Space) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.NameIdMap[space.Name] = space.Id
	c.spaces[space.Id] = space
}

func (c *SpaceCache) deleteSpace(space *Space) {
	c.lock.Lock()
	defer c.lock.Unlock()

	oldSpace, ok := c.spaces[space.Id]
	if !ok {
		return
	}
	delete(c.NameIdMap, oldSpace.Name)
}
