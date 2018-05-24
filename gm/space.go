package gm

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/deepcopy"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
)

type PartitionPolicy struct {
	Key      string
	Function string
	Number   uint64
}

type Space struct {
	*metapb.Space
	searchTree   *PartitionTree `json:"-"`
	propertyLock sync.RWMutex   `json:"-"`
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}

func NewSpace(dbId metapb.DBID, dbName, spaceName string, policy *PartitionPolicy) (*Space, error) {
	spaceId, err := GetIdGeneratorSingle().GenID()
	if err != nil {
		log.Error("generate space id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}

	metaSpace := &metapb.Space{
		Name:   spaceName,
		ID:     metapb.SpaceID(spaceId),
		DB:     dbId,
		DbName: dbName,
		Status: metapb.SS_Init,
		KeyPolicy: &metapb.KeyPolicy{
			KeyField: policy.Key,
			KeyFunc:  policy.Function,
		},
	}
	return NewSpaceByMeta(metaSpace), nil
}

func NewSpaceByMeta(metaSpace *metapb.Space) *Space {
	return &Space{
		Space:      metaSpace,
		searchTree: NewPartitionTree(),
	}
}

func (s *Space) persistent() error {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	copy := deepcopy.Iface(s.Space).(*metapb.Space)
	spaceVal, err := proto.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal space[%v]. err:[%v]", copy, err)
		return err
	}
	// TODO 调用global etcd添加space, 接口由@杨洋提供
	return nil
}

func (s *Space) erase() error {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	// TODO 调用global etcd删除space, 接口由@杨洋提供
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

	s.searchTree.update(partition)
}

func (s *Space) AscendScanPartition(pivotSlot metapb.SlotID, batchNum int) []*Partition {
	searchPivot := &Partition{
		Partition: &metapb.Partition{
			StartSlot: pivotSlot,
		},
	}
	items := s.searchTree.ascendScan(searchPivot, batchNum)
	if items == nil || len(items) == 0 {
		return nil
	}

	result := make([]*Partition, 0, len(items))
	for _, item := range items {
		result = append(result, item.partition)
	}
	return result
}

type SpaceCache struct {
	lock     sync.RWMutex
	name2Ids map[string]metapb.SpaceID
	spaces   map[metapb.SpaceID]*Space
}

func NewSpaceCache() *SpaceCache {
	return &SpaceCache{
		name2Ids: make(map[string]metapb.SpaceID),
		spaces:   make(map[metapb.SpaceID]*Space),
	}
}

func (c *SpaceCache) FindSpaceByName(spaceName string) *Space {
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

func (c *SpaceCache) FindSpaceById(spaceId metapb.SpaceID) *Space {
	c.lock.RLock()
	defer c.lock.RUnlock()

	space, ok := c.spaces[spaceId]
	if !ok {
		return nil
	}
	return space
}

func (c *SpaceCache) GetAllSpaces() []*Space {
	c.lock.RLock()
	defer c.lock.RUnlock()

	spaces := make([]*Space, 0, len(c.spaces))
	for _, space := range c.spaces {
		spaces = append(spaces, space)
	}

	return spaces
}

func (c *SpaceCache) AddSpace(space *Space) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.name2Ids[space.Name] = space.ID
	c.spaces[space.ID] = space
}

func (c *SpaceCache) DeleteSpace(space *Space) {
	c.lock.Lock()
	defer c.lock.Unlock()

	oldSpace, ok := c.spaces[space.ID]
	if !ok {
		return
	}
	delete(c.name2Ids, oldSpace.Name)
}

func (c *SpaceCache) Recovery() ([]*Space, error) {
	resultSpaces := make([]*Space, 0)

	// TODO 从global etcd里获得一个DB的space list, 由@杨洋提供接口
	topoSpaces := make([]metapb.Space, 0)
	for _, topoSpace := range topoSpaces {
		err := proto.Unmarshal([]byte{}, topoSpace)
		if err != nil {
			log.Error("proto.Unmarshal error, err:[%v]", err)
		}
		metaSpace := new(metapb.Space)
		metaSpace.ID = topoSpace.ID
		metaSpace.Name = topoSpace.Name
		metaSpace.DB = topoSpace.DB
		metaSpace.Type = topoSpace.Type
		metaSpace.DbName = topoSpace.DbName
		metaSpace.KeyPolicy = topoSpace.KeyPolicy
		metaSpace.Status = topoSpace.Status

		resultSpaces = append(resultSpaces, NewSpaceByMeta(metaSpace))
	}
	return resultSpaces, nil
}
