package master

import (
	"util/log"
	"sync"
	"math"
)

type Cluster struct {
	config 			*Config
	store 			Store

	dbCache 		*DBCache
	psCache 		*PSCache
	partitionCache  *PartitionCache

    clusterLock     sync.RWMutex
}

func NewCluster(config *Config) *Cluster {
	return &Cluster{
		config: 		config,
		store: 			NewRaftStore(config),
		dbCache: 		NewDBCache(),
		psCache:        NewPSCache(),
		partitionCache: NewPartitionCache(),
	}
}

func (c *Cluster) Start() error {
	if err := c.store.Open(); err != nil {
		log.Error("fail to create raft store. err:[%v]", err)
		return err
	}

	GetIdGeneratorInstance(c.store)

	// recovery memory meta data
	if err := c.recoveryPSCache(); err != nil {
		log.Error("fail to recovery psCache. err:[%v]", err)
		return err
	}
	if err := c.recoveryDBCache(); err != nil {
		log.Error("fail to recovery dbCache. err[%v]", err)
		return err
	}

	return nil
}

func (c *Cluster) Close() {
	if c.store != nil {
		c.store.Close()
	}
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

	c.dbCache.recovery()
	dbs, err := c.spaceCache.recovery(c.store)
	if err != nil {
		return err
	}

	for _, db := range dbs {
		c.dbCache.addDb(db)
	}

	return nil
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

	// batch commit
	batch := c.store.NewBatch()

	policy := &PartitionPolicy{
		Key: partitionKey,
		Function:partitionFunc,
		NumPartitions:partitionNum,
	}
	space, err := NewSpace(db.ID, dbName, spaceName, policy)
	if err != nil {
		return nil, err
	}
	if err := space.batchPersistent(batch); err != nil {
		return nil, err
	}

	slots := slotSplit(0, math.MaxUint32, partitionNum + 1)
	if slots == nil {
		log.Error("fail to split slot range [%v-%v]", 0, math.MaxUint32)
		return nil, ErrInternalError
	}
	partitions := make([]*Partition, len(slots))
	for i := 0; i < len(slots) - 1; i++ {
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
		return nil, ErrBoltDbOpsFailed
	}

	// update memory and send event
	db.spaceCache.addSpace(space)
	for _, partition := range partitions {
		space.putPartition(partition)

		if err := PushProcessorEvent(NewPartitionCreateEvent(partition)); err != nil {
			log.Error("fail to push event for creating partition[%v].", partition)
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

func (c *Cluster) createPS(ip string) (*PartitionServer, error) {


    newPs := NewPartitionServer(newId, request.Ip)
    newPs.changeStatus(PS_Registered)
    rs.cluster.psCache.addServer(ps)
    newPs.persistent()
    resp.NodeID = newId
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
