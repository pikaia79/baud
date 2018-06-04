package zm

import (
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
		PartitionTopo: &topo.PartitionTopo{Partition: &metapb.Partition{
			StartSlot: pivotSlot,
		}},
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
func (space *Space) Update(spaceTopo *topo.SpaceTopo) {
	space.propertyLock.Lock()
	defer space.propertyLock.Unlock()

	space.SpaceTopo = spaceTopo
}

type SpaceCache struct {
	lock        sync.RWMutex
	name2Ids    map[string]metapb.SpaceID
	spaces      map[metapb.SpaceID]*Space
	cancelWatch topo.CancelFunc
}

func NewSpaceCache() *SpaceCache {
	return &SpaceCache{
		name2Ids: make(map[string]metapb.SpaceID),
		spaces:   make(map[metapb.SpaceID]*Space),
	}
}

func (sc *SpaceCache) FindSpaceByName(spaceName string) *Space {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	spaceId, ok := sc.name2Ids[spaceName]
	if !ok {
		return nil
	}
	space, ok := sc.spaces[spaceId]
	if !ok {
		log.Error("!!!space cache map not consistent, space[%v : %v] not exists. never happened", spaceName, spaceId)
		return nil
	}
	return space
}

func (sc *SpaceCache) FindSpaceById(spaceId metapb.SpaceID) *Space {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	space, ok := sc.spaces[spaceId]
	if !ok {
		return nil
	}
	return space
}

func (sc *SpaceCache) GetAllSpaces() []*Space {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	spaces := make([]*Space, 0, len(sc.spaces))
	for _, space := range sc.spaces {
		spaces = append(spaces, space)
	}

	return spaces
}

func (sc *SpaceCache) AddSpace(space *Space) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	sc.name2Ids[space.Name] = space.ID
	sc.spaces[space.ID] = space
}

func (sc *SpaceCache) DeleteSpace(space *Space) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	oldSpace, ok := sc.spaces[space.ID]
	if !ok {
		return
	}
	delete(sc.name2Ids, oldSpace.Name)
}
