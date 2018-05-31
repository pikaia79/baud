package zm

import (
	"context"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
)

const (
	PREFIX_SPACE = "schema space "
)

type PartitionPolicy struct {
	Key      string
	Function string
	Number   uint32
}

type Space struct {
	*topo.SpaceTopo
	parent       *DB
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

func NewSpaceByMeta(metaSpace *topo.SpaceTopo) *Space {
	return &Space{
		SpaceTopo:  metaSpace,
		searchTree: NewPartitionTree(),
	}
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

func (c *SpaceCache) Recovery(topoServer *topo.TopoServer) ([]*Space, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TOPO_TIMEOUT)
	defer cancel()

	spaces, err := topoServer.GetAllSpaces(ctx)
	if err != nil {
		log.Error("topoServer.GetAllSpaces() failed: %s", err.Error())
	}

	resultSpaces := make([]*Space, 0)
	for _, space := range spaces {
		resultSpaces = append(resultSpaces, NewSpaceByMeta(space))
	}

	return resultSpaces, nil
}
