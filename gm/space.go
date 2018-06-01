package gm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"sync"
)

type PartitionPolicy struct {
	Key      string
	Function string
	Number   uint64
}

type Space struct {
	*topo.SpaceTopo
	partitions   map[metapb.PartitionID]*Partition
	propertyLock sync.RWMutex `json:"-"`
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}

func NewSpace(dbId metapb.DBID, dbName, spaceName, spaceSchema string, policy *PartitionPolicy) (*Space, error) {
	spaceId, err := GetIdGeneratorSingle().GenID()
	if err != nil {
		log.Error("generate space id is failed. err:[%v]", err)
		return nil, ErrGenIdFailed
	}

	spaceMeta := &metapb.Space{
		Name:   spaceName,
		Schema: spaceSchema,
		ID:     metapb.SpaceID(spaceId),
		DB:     dbId,
		DbName: dbName,
		Status: metapb.SS_Init,
		KeyPolicy: &metapb.KeyPolicy{
			KeyField: policy.Key,
			KeyFunc:  policy.Function,
		},
	}

	spaceTopo := &topo.SpaceTopo{
		Space: spaceMeta,
	}

	return NewSpaceByTopo(spaceTopo), nil
}

func NewSpaceByTopo(spaceTopo *topo.SpaceTopo) *Space {
	return &Space{
		SpaceTopo: spaceTopo,
	}
}

func (s *Space) add(partitions []*Partition) error {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	partitionsMeta := make([]*metapb.Partition, 0)
	for _, partition := range partitions {
		partitionsMeta = append(partitionsMeta, partition.PartitionTopo.Partition)
	}

	spaceTopo, partitionsTopo, err := topoServer.AddSpace(ctx, s.SpaceTopo.Space, partitionsMeta)
	if err != nil {
		log.Error("topoServer AddSpace error, err: [%v]", err)
		return err
	}
	s.SpaceTopo = spaceTopo
	partitionsMap := make(map[metapb.PartitionID]*Partition)
	for _, partitionTopo := range partitionsTopo {
		partition := &Partition{
			PartitionTopo: partitionTopo,
		}
		partitionsMap[partitionTopo.ID] = partition
	}
	s.partitions = partitionsMap

	return nil
}

func (s *Space) update() error {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	err := topoServer.UpdateSpace(ctx, s.SpaceTopo)
	if err != nil {
		log.Error("topoServer UpdateSpace error, err: [%v]", err)
		return err
	}

	return nil
}

func (s *Space) erase() error {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	// TODO partition是否在删除space时一起删除???
	err := topoServer.DeleteSpace(ctx, s.SpaceTopo)
	if err != nil {
		log.Error("topoServer DeleteSpace error, err: [%v]", err)
		return err
	}
	return nil
}

func (s *Space) rename(newName string) {
	s.propertyLock.Lock()
	defer s.propertyLock.Unlock()

	s.Name = newName
}

// SpaceCache

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
	delete(c.spaces, space.ID)
	delete(c.name2Ids, oldSpace.Name)
}

func (c *SpaceCache) Recovery() ([]*Space, error) {
	resultSpaces := make([]*Space, 0)

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	spacesTopo, err := topoServer.GetAllSpaces(ctx)
	if err != nil {
		log.Error("topoServer GetAllSpaces error, err: [%v]", err)
		return nil, err
	}
	if spacesTopo != nil {
		for _, spaceTopo := range spacesTopo {
			space := &Space{
				SpaceTopo: spaceTopo,
			}
			resultSpaces = append(resultSpaces, space)
		}
	}

	return resultSpaces, nil
}
