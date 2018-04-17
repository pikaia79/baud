package master

import (
	"util/log"
	"sync"
	"math"
)

type Cluster struct {
	config 			*Config
	store 			Store

	clusterLock     sync.RWMutex
	dbCache 		*DBCache
	psCache 		*PSCache
	partitionCache  *PartitionCache
}

func NewCluster(config *Config) *Cluster {
	return &Cluster{
		config: 		config,
		store: 			NewRaftStore(config),
		dbCache: 		new(DBCache),
	}
}

func (c *Cluster) Start() (err error) {
	if err = c.store.Open(); err != nil {
		log.Error("fail to create raft store. err:[%v]", err)
		return
	}

	GetIdGeneratorInstance(c.store)

	return nil
}

func (c *Cluster) Close() {
	if c.store != nil {
		c.store.Close()
	}
}

func (c *Cluster) createDb(dbName string) (*DB, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.dbCache.findDbByName(dbName)
	if db != nil {
		return nil, ErrDupDb
	}

	db, err := NewDB(dbName)
	if err != nil {
		return nil, err
	}

	if err := db.persistent(c.store); err != nil {
		return nil, err
	}
	c.dbCache.addDb(db)

	return db, nil
}

func (c *Cluster) renameDb(srcDbName, destDbName string) error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	srcDb := c.dbCache.findDbByName(srcDbName)
	if srcDb == nil {
		return ErrDbNotExists
	}
	destDb := c.dbCache.findDbByName(destDbName)
	if destDb != nil {
		return ErrDupDb
	}

	c.dbCache.deleteDb(srcDb)
	srcDb.rename(destDbName)
	if err := srcDb.persistent(c.store); err != nil {
		return err
	}
	c.dbCache.addDb(srcDb)

	return nil
}

func (c *Cluster) createSpace(dbName, spaceName, partitionKey, partitionFunc string, partitionNum int) (*Space, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.dbCache.findDbByName(dbName)
	if db == nil {
		return nil, ErrDbNotExists
	}
	if space := db.spaceCache.findSpaceByName(spaceName); space != nil {
		return nil, ErrDupSpace
	}

	policy := &PartitionPolicy{
		Key: partitionKey,
		Function:partitionFunc,
		NumPartitions:partitionNum,
	}
	space, err := NewSpace(db.Id, dbName, spaceName, policy)
	if err != nil {
		return nil, err
	}

	if err := space.persistent(c.store); err != nil {
		return nil, err
	}
	db.spaceCache.addSpace(space)

	slots := slotSplit(0, math.MaxUint32, partitionNum + 1)
	if slots == nil {
		log.Error("fail to split slot range [%v-%v]", 0, math.MaxUint32)
		return nil, ErrInternalError
	}
	for i := 0; i < len(slots) - 1; i++ {
		partition, err := NewPartition(db.Id, space.Id, slots[i], slots[i+1])
		if err != nil {
			return nil, err
		}

		if err := partition.persistent(c.store); err != nil {
			return nil, err
		}
		space.putPartition(partition)

		if err := PushProcessorEvent(partition); err != nil {
			return
		}
	}

	return space, nil
}

func (c *Cluster) renameSpace(dbName, srcSpaceName, destSpaceName string) error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.dbCache.findDbByName(dbName)
	if db == nil {
		return ErrDbNotExists
	}

	srcSpace := db.spaceCache.findSpaceByName(srcSpaceName)
	if srcSpace == nil {
		return ErrSpaceNotExists
	}
	destSpace := db.spaceCache.findSpaceByName(destSpaceName)
	if destSpace != nil {
		return ErrDupSpace
	}

	db.spaceCache.deleteSpace(srcSpace)
	srcSpace.rename(destSpaceName)
	if err := srcSpace.persistent(c.store); err != nil {
		return err
	}
	db.spaceCache.addSpace(srcSpace)

	return nil
}

func (c *Cluster) detailSpace(spaceId int) (*Space, error) {
	return nil, nil
}

func slotSplit(start, end uint32, n int) []uint32 {
	if n <= 0 {
		return nil
	}
	if end - start + 1 < uint32(n) {
		return nil
	}

	var min, max uint32
	if start <= end {
		min = start
		max = end
	} else {
		min = end
		max = start
	}

	ret := make([]uint32, 0)
	switch n {
	case 1:
		ret = append(ret, min)
	case 2:
		ret = append(ret, min)
		ret = append(ret, max)
	default:
		step := (max - min) / uint32(n - 1)
		ret = append(ret, min)
		for i := 1 ; i < n - 1; i++ {
			ret = append(ret, min + uint32(i) * step)
		}
		ret = append(ret, max)
	}

	return ret
}
