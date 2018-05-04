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

	DbCache        *DBCache
	PsCache        *PSCache
	PartitionCache *PartitionCache

	clusterLock sync.RWMutex
}

func NewCluster(config *Config, store Store) *Cluster {
	return &Cluster{
		config:         config,
		store:          store,
		DbCache:        NewDBCache(),
		PsCache:        NewPSCache(),
		PartitionCache: NewPartitionCache(),
	}
}

func (c *Cluster) Start() error {
	// recovery memory meta data
	if err := c.recoveryPSCache(); err != nil {
		log.Error("fail to recovery PsCache. err:[%v]", err)
		return err
	}
	if err := c.recoveryDBCache(); err != nil {
		log.Error("fail to recovery DbCache. err[%v]", err)
		return err
	}
	if err := c.recoverySpaceCache(); err != nil {
		log.Error("fail to recovery SpaceCache. err[%v]", err)
		return err
	}
	if err := c.recoveryPartitionCache(); err != nil {
		log.Error("fail to recovery PartitionCache. err[%v]", err)
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

	servers, err := c.PsCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, server := range servers {
		c.PsCache.addServer(server)
	}

	return nil
}

func (c *Cluster) recoveryDBCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	dbs, err := c.DbCache.Recovery(c.store)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		c.DbCache.AddDb(db)
	}

	return nil
}

func (c *Cluster) recoverySpaceCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	dbs := c.DbCache.GetAllDBs()
	if dbs == nil || len(dbs) == 0 {
		return nil
	}

	allSpaces, err := dbs[0].SpaceCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, space := range allSpaces {
		db := c.DbCache.FindDbById(space.DB)
		if db == nil {
			log.Warn("Cannot find db for the space[%v] when recovery space. discord it", space)

			if err := space.erase(c.store); err != nil {
				log.Error("fail to remove unused space[%v] when recovery. err:[%v]", space, err)
			}
			continue
		}

		db.SpaceCache.addSpace(space)
	}

	return nil
}

func (c *Cluster) recoveryPartitionCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	partitions, err := c.PartitionCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		db := c.DbCache.FindDbById(partition.DB)
		if db == nil {
			log.Warn("Cannot find db for the partition[%v] when recovery partition. discord it", partition)

			if err := partition.erase(c.store); err != nil {
				log.Error("fail to remove unused partition[%v] when recovery. err:[%v]", partition, err)
			}
			continue
		}

		space := db.SpaceCache.findSpaceById(partition.Space)
		if space == nil {
			log.Warn("Cannot find space for the partition[%v] when recovery partition. discord it", partition)

			if err := partition.erase(c.store); err != nil {
				log.Error("fail to remove unused partition[%v] when recovery. err:[%v]", partition, err)
			}
			continue
		}

		space.searchTree.update(partition)
		c.PartitionCache.AddPartition(partition)

		var delMetaReplicas = make([]*metapb.Replica, 0)
		for _, metaReplica := range partition.getAllReplicas() {
			ps := c.PsCache.findServerById(metaReplica.NodeID)
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

	c.PsCache.Clear()
	c.DbCache.Clear()
	c.PartitionCache.Clear()
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
	if space := db.SpaceCache.findSpaceByName(spaceName); space != nil {
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
	db.SpaceCache.addSpace(space)
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

	srcSpace := db.SpaceCache.findSpaceByName(srcSpaceName)
	if srcSpace == nil {
		return ErrSpaceNotExists
	}
	destSpace := db.SpaceCache.findSpaceByName(destSpaceName)
	if destSpace != nil {
		return ErrDupSpace
	}

	db.SpaceCache.deleteSpace(srcSpace)
	srcSpace.rename(destSpaceName)
	if err := srcSpace.persistent(c.store); err != nil {
		return err
	}
	db.SpaceCache.addSpace(srcSpace)

	return nil
}
