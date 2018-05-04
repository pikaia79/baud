package master

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"math"
	"sync"
)

type Cluster struct {
	config *Config
	store  Store

	dbCache        *DBCache
	psCache        *PSCache
	partitionCache *PartitionCache

	clusterLock sync.RWMutex
}

func NewCluster(config *Config, store Store) *Cluster {
	return &Cluster{
		config:         config,
		store: 			store,
		dbCache:        NewDBCache(),
		psCache:        NewPSCache(),
		partitionCache: NewPartitionCache(),
	}
}

func (c *Cluster) Start() error {
	// recovery memory meta data
	if err := c.recoveryPSCache(); err != nil {
		log.Error("fail to recovery psCache. err:[%v]", err)
		return err
	}
	if err := c.recoveryDBCache(); err != nil {
		log.Error("fail to recovery dbCache. err[%v]", err)
		return err
	}
	if err := c.recoverySpaceCache(); err != nil {
		log.Error("fail to recovery spaceCache. err[%v]", err)
		return err
	}
	if err := c.recoveryPartitionCache(); err != nil {
		log.Error("fail to recovery partitionCache. err[%v]", err)
		return err
	}
	log.Info("finish to recovery whole cluster")

	log.Info("Cluster has started")
	return nil
}

func (c *Cluster) Close() {
	c.clearAllCache()
	log.Info("Cluster has closed")
}

func (c *Cluster) recoveryPSCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	servers, err := c.psCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, server := range servers {
		c.psCache.addServer(server)
	}

	return nil
}

func (c *Cluster) recoveryDBCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	dbs, err := c.dbCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		c.dbCache.addDb(db)
	}

	return nil
}

func (c *Cluster) recoverySpaceCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	dbs := c.dbCache.getAllDBs()
	if dbs == nil || len(dbs) == 0 {
		return nil
	}

	allSpaces, err := dbs[0].spaceCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, space := range allSpaces {
		db := c.dbCache.findDbById(space.DB)
		if db == nil {
			log.Warn("Cannot find db for the space[%v] when recovery space. discord it", space)

			if err := space.erase(c.store); err != nil {
				log.Error("fail to remove unused space[%v] when recovery. err:[%v]", space, err)
			}
			continue
		}

		db.spaceCache.addSpace(space)
	}

	return nil
}

func (c *Cluster) recoveryPartitionCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	partitions, err := c.partitionCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		db := c.dbCache.findDbById(partition.DB)
		if db == nil {
			log.Warn("Cannot find db for the partition[%v] when recovery partition. discord it", partition)

			if err := partition.erase(c.store); err != nil {
				log.Error("fail to remove unused partition[%v] when recovery. err:[%v]", partition, err)
			}
			continue
		}

		space := db.spaceCache.findSpaceById(partition.Space)
		if space == nil {
			log.Warn("Cannot find space for the partition[%v] when recovery partition. discord it", partition)

			if err := partition.erase(c.store); err != nil {
				log.Error("fail to remove unused partition[%v] when recovery. err:[%v]", partition, err)
			}
			continue
		}

		space.searchTree.update(partition)
		c.partitionCache.addPartition(partition)

		var delMetaReplicas = make([]*metapb.Replica, 0)
		for _, metaReplica := range partition.getAllReplicas() {
			ps := c.psCache.findServerById(metaReplica.NodeID)
			if ps == nil {
				log.Warn("Cannot find ps for the replica[%v] when recovery replicas. discord it", metaReplica)
				delMetaReplicas = append(delMetaReplicas, metaReplica)
				continue
			}

			ps.addPartition(partition)
		}
		if err := partition.deleteReplica(c.store, delMetaReplicas...); err != nil {
			log.Error("fail to remove unused replicas when recovery partition[%v]. err[%v]", partition, err)
			continue
		}
	}

	return nil
}

func (c *Cluster) clearAllCache() {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	c.psCache.clear()
	c.dbCache.clear()
	c.partitionCache.clear()
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

func (c *Cluster) createSpace(dbName, spaceName string, policy *PartitionPolicy) (*Space, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.dbCache.findDbByName(dbName)
	if db == nil {
		return nil, ErrDbNotExists
	}
	if space := db.spaceCache.findSpaceByName(spaceName); space != nil {
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
		partition, err := NewPartition(db.ID, space.ID, slots[i], slots[i+1])
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
	db.spaceCache.addSpace(space)
	for _, partition := range partitions {
		space.putPartition(partition)

		//if err := PushProcessorEvent(NewPartitionCreateEvent(partition)); err != nil {
		//	log.Error("fail to push event for creating partition[%v].", partition)
		//}
	}

	// waiting to continues to create partition in ps by the background worker

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
