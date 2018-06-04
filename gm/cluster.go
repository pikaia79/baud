package gm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"math"
	"sync"
)

type Cluster struct {
	config *Config
	gm     *GM

	DbCache        *DBCache
	PartitionCache *PartitionCache

	clusterLock sync.RWMutex
}

func NewCluster(config *Config, gm *GM) *Cluster {
	return &Cluster{
		config:         config,
		gm:             gm,
		DbCache:        NewDBCache(),
		PartitionCache: NewPartitionCache(),
	}
}

func (c *Cluster) Start() error {
	// recovery memory meta data
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

func (c *Cluster) recoveryDBCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	dbs, err := c.DbCache.Recovery()
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

	for _, db := range dbs {
		allSpaces, err := db.SpaceCache.Recovery()
		if err != nil {
			return err
		}

		for _, space := range allSpaces {
			dbTemp := c.DbCache.FindDbById(space.DB)
			if dbTemp == nil {
				log.Warn("Cannot find db for the space[%v] when recovery space. discord it", space)
				if err := space.erase(); err != nil {
					log.Error("fail to remove unused space[%v] when recovery. err:[%v]", space, err)
				}
			}
			db.SpaceCache.AddSpace(space)
		}
	}
	return nil
}

func (c *Cluster) recoveryPartitionCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	partitions, err := c.PartitionCache.Recovery()
	if err != nil {
		return err
	}

	for _, partition := range partitions {
		db := c.DbCache.FindDbById(partition.DB)
		if db == nil {
			log.Warn("Cannot find db for the partition[%v] when recovery partition. discord it", partition)
			continue
		}

		space := db.SpaceCache.FindSpaceById(partition.Space)
		if space == nil {
			log.Warn("Cannot find space for the partition[%v] when recovery partition. discord it", partition)
			continue
		}

		c.PartitionCache.AddPartition(partition)
	}

	return nil
}

func (c *Cluster) watchZonesTopo() error {

	return nil
}

func (c *Cluster) clearAllCache() {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	c.PartitionCache.Clear()
	// SpaceCache in DbCache
	c.DbCache.Clear()
}

// zone, db, space, parititon
// zone
func (c *Cluster) CreateZone(zoneName, zoneEtcdAddr, zoneRootDir string) (*Zone, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zoneTopo, err := TopoServer.GetZone(ctx, zoneName)
	if err != nil {
		log.Error("TopoServer GetZone error, err: [%v]", err)
		return nil, err
	}
	if zoneTopo != nil {
		return nil, ErrDupZone
	}

	zone, err := NewZone(zoneName, zoneEtcdAddr, zoneRootDir)
	if err != nil {
		return nil, err
	}

	if err := zone.add(); err != nil {
		return nil, err
	}

	return zone, nil
}

func (c *Cluster) DeleteZone(zoneName string) error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zoneTopo, err := TopoServer.GetZone(ctx, zoneName)
	if err != nil {
		log.Error("TopoServer GetZone error, err: [%v]", err)
		return err
	}
	if zoneTopo == nil {
		return ErrZoneNotExists
	}
	zone := &Zone{
		ZoneTopo: zoneTopo,
	}
	if err := zone.erase(); err != nil {
		return err
	}

	return nil
}

func (c *Cluster) GetAllZones() ([]*Zone, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	zones := make([]*Zone, 0)
	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zonesTopo, err := TopoServer.GetAllZones(ctx)
	if err != nil {
		log.Error("TopoServer GetAllZones error, err: [%v]", err)
		return nil, err
	}
	for _, zoneTopo := range zonesTopo {
		zones = append(zones, &Zone{ZoneTopo: zoneTopo})
	}
	return zones, nil
}

func (c *Cluster) GetAllZonesMap() (map[string]*Zone, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	zonesMap := make(map[string]*Zone)
	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zonesTopo, err := TopoServer.GetAllZones(ctx)
	if err != nil {
		log.Error("TopoServer GetAllZones error, err: [%v]", err)
		return nil, err
	}
	for _, zoneTopo := range zonesTopo {
		zonesMap[zoneTopo.Name] = &Zone{ZoneTopo: zoneTopo}
	}
	return zonesMap, nil
}

func (c *Cluster) GetAllZonesName() ([]string, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	zonesName := make([]string, 0)
	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zonesTopo, err := TopoServer.GetAllZones(ctx)
	if err != nil {
		log.Error("TopoServer GetAllZones error, err: [%v]", err)
		return nil, err
	}
	for _, zoneTopo := range zonesTopo {
		zonesName = append(zonesName, zoneTopo.Name)
	}
	return zonesName, nil
}

func (c *Cluster) GetZone(zoneName string) (*Zone, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zoneTopo, err := TopoServer.GetZone(ctx, zoneName)
	if err != nil {
		log.Error("TopoServer GetZone error, err: [%v]", err)
		return nil, err
	}
	return &Zone{ZoneTopo: zoneTopo}, nil
}

// db
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

	if err := db.add(); err != nil {
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
	if err := srcDb.update(); err != nil {
		return err
	}
	c.DbCache.AddDb(srcDb)

	return nil
}

func (c *Cluster) DeleteDb(dbName string) error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.DbCache.FindDbByName(dbName)
	if db == nil {
		return ErrDbNotExists
	}

	if err := db.erase(); err != nil {
		return err
	}
	c.DbCache.DeleteDb(db)

	return nil
}

// space
func (c *Cluster) CreateSpace(dbName, spaceName, spaceSchema string, policy *PartitionPolicy) (*Space, error) {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.DbCache.FindDbByName(dbName)
	if db == nil {
		return nil, ErrDbNotExists
	}
	if space := db.SpaceCache.FindSpaceByName(spaceName); space != nil {
		return nil, ErrDupSpace
	}

	space, err := NewSpace(db.ID, dbName, spaceName, spaceSchema, policy)
	if err != nil {
		return nil, err
	}

	slots := util.SlotSplit(0, math.MaxUint32, policy.Number+1)
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
	}

	if err := space.add(partitions); err != nil {
		return nil, err
	}

	db.SpaceCache.AddSpace(space)
	for _, partition := range partitions {
		c.PartitionCache.AddPartition(partition)
	}

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
	if err := srcSpace.update(); err != nil {
		return err
	}
	db.SpaceCache.AddSpace(srcSpace)

	return nil
}

func (c *Cluster) DeleteSpace(dbName, spaceName string) error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	db := c.DbCache.FindDbByName(dbName)
	if db == nil {
		return ErrDbNotExists
	}
	space := db.SpaceCache.FindSpaceByName(spaceName)
	if space == nil {
		return ErrSpaceNotExists
	}

	if err := space.erase(); err != nil {
		return err
	}
	db.SpaceCache.DeleteSpace(space)
	for _, partition := range space.partitions {
		c.PartitionCache.DeletePartition(partition.PartitionTopo.Partition.ID)
	}

	return nil
}
