package gm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"math"
	"sync"
)

type Cluster struct {
	config      *Config
	clusterLock sync.RWMutex
}

func NewCluster(config *Config) *Cluster {
	return &Cluster{
		config: config,
	}
}

func (c *Cluster) Start() error {
	log.Info("Cluster has started")
	return nil
}

func (c *Cluster) Close() {
	log.Info("Cluster has closed")
}

func (c *Cluster) CreateDb(dbName string) (*DB, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.DbCache.FindDbByName(dbName)
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
	c.DbCache.AddDb(db)

	return db, nil
}

func (c *Cluster) RenameDb(srcDbName, destDbName string) error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	srcDb := c.DbCache.FindDbByName(srcDbName)
	if srcDb == nil {
		return ErrDbNotExists
	}
	destDb := c.DbCache.FindDbByName(destDbName)
	if destDb != nil {
		return ErrDupDb
	}

	c.DbCache.DeleteDb(srcDb)
	srcDb.rename(destDbName)
	if err := srcDb.persistent(c.store); err != nil {
		return err
	}
	c.DbCache.AddDb(srcDb)

	return nil
}

func (c *Cluster) CreateSpace(dbName, spaceName string, policy *PartitionPolicy) (*Space, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.DbCache.FindDbByName(dbName)
	if db == nil {
		return nil, ErrDbNotExists
	}
	if space := db.SpaceCache.FindSpaceByName(spaceName); space != nil {
		return nil, ErrDupSpace
	}

	// batch commit
	batch := c.store.NewBatch()

	space, err := NewSpace(db.ID, dbName, spaceName, policy)
	if err != nil {
		return nil, err
	}
	if err := space.batchPersistent(batch); err != nil {
		return nil, err
	}

	slots := util.SlotSplit(0, math.MaxUint32, uint64(policy.Number)+1)
	if slots == nil {
		log.Error("fail to split slot range [%v-%v]", 0, math.MaxUint32)
		return nil, ErrInternalError
	}
	partitions := make([]*Partition, 0, len(slots))
	for i := 0; i < len(slots)-1; i++ {
		partition, err := NewPartition(db.ID, space.ID, metapb.SlotID(slots[i]), metapb.SlotID(slots[i+1]))
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
		if err := partition.batchPersistent(batch); err != nil {
			return nil, err
		}
	}
	if err := batch.Commit(); err != nil {
		return nil, ErrLocalDbOpsFailed
	}

	// update memory and send event
	db.SpaceCache.AddSpace(space)
	for _, partition := range partitions {
		space.putPartition(partition)

		//if err := PushProcessorEvent(NewPartitionCreateEvent(partition)); err != nil {
		//	log.Error("fail to push event for creating partition[%v].", partition)
		//}
	}

	// waiting to continues to create partition in ps by the background worker

	return space, nil
}

func (c *Cluster) RenameSpace(dbName, srcSpaceName, destSpaceName string) error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.DbCache.FindDbByName(dbName)
	if db == nil {
		return ErrDbNotExists
	}

	srcSpace := db.SpaceCache.FindSpaceByName(srcSpaceName)
	if srcSpace == nil {
		return ErrSpaceNotExists
	}
	destSpace := db.SpaceCache.FindSpaceByName(destSpaceName)
	if destSpace != nil {
		return ErrDupSpace
	}

	db.SpaceCache.DeleteSpace(srcSpace)
	srcSpace.rename(destSpaceName)
	if err := srcSpace.persistent(c.store); err != nil {
		return err
	}
	db.SpaceCache.AddSpace(srcSpace)

	return nil
}
