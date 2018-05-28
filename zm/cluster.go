package zm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"math"
	"sync"
	"github.com/tiglabs/baudengine/topo"
)

type Cluster struct {
	config *Config
	topoServer *topo.TopoServer

	spaceMap sync.Map
	// timeWheel to check timeout and refresh cache
	timeWheel TimeWheel
	DbCache        *DBCache
	PsCache        *PSCache

	clusterLock sync.RWMutex
}

func NewCluster(config *Config, topoServer *topo.TopoServer) *Cluster {
	return &Cluster{
		config:         config,
		topoServer:          topoServer,
		DbCache:        NewDBCache(),
		PsCache:        NewPSCache(),
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
	c.topoServer.GetAllZones()
	log.Info("Cluster has closed")
}

func (c *Cluster) recoveryPSCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	servers, err := c.PsCache.Recovery(c.store, &c.config.PsCfg)
	if err != nil {
		return err
	}

	for _, server := range servers {
		c.PsCache.AddServer(server)
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

	allSpaces, err := dbs[0].SpaceCache.Recovery(c.store)
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

		db.SpaceCache.AddSpace(space)
	}

	return nil
}

func (c *Cluster) recoveryPartitionCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	partitions, err := c.PartitionCache.Recovery(c.store)
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

		space := db.SpaceCache.FindSpaceById(partition.Space)
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
			ps := c.PsCache.FindServerById(metaReplica.NodeID)
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

