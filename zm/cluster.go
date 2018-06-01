package zm

import (
	"context"
	"github.com/pkg/errors"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
)

type Cluster struct {
	config     *Config
	topoServer *topo.TopoServer
	masterCtx  context.Context

	spaceMap sync.Map
	// timeWheel to check timeout and refresh cache
	timeWheel      TimeWheel
	DbCache        *DBCache
	PsCache        *PSCache
	PartitionCache *PartitionCache

	cancelDBWatch    topo.CancelFunc
	cancelSpaceWatch topo.CancelFunc

	clusterLock sync.RWMutex
}

func NewCluster(ctx context.Context, config *Config, topoServer *topo.TopoServer) *Cluster {
	return &Cluster{
		config:     config,
		masterCtx:  ctx,
		topoServer: topoServer,
		DbCache:    NewDBCache(),
		PsCache:    NewPSCache(),
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

func (c *Cluster) watchDBChange() []*topo.DBTopo {
	err, currentDbTopos, dbChannel, cancel := c.topoServer.WatchDBs(c.masterCtx)
	if err != nil {
		log.Debug("WatchDBs err[%v]", err)
		return nil
	}
	c.cancelDBWatch = cancel
	log.Debug("current dbs[%v] before WatchDBs", currentDbTopos)

	go func() {
		for db := range dbChannel {
			if db.Err != nil {
				if db.Err == topo.ErrNoNode {
					oldDB := c.DbCache.FindDbById(db.ID)
					if oldDB != nil {
						c.DbCache.DeleteDb(oldDB)
					}
					continue
				}
				log.Error("watch err[%v]", db.Err)
				return
			}
			log.Debug("watched db[%v]", db.DB)
			oldDB := c.DbCache.FindDbById(db.ID)
			if oldDB == nil {
				c.DbCache.AddDb(NewDBByMeta(c, db.DBTopo))
			} else {
				oldDB.Update(db.DBTopo)
			}
		}
	}()

	return currentDbTopos
}

func (c *Cluster) WatchSpaceChange() []*topo.SpaceTopo {
	err, currentSpaceTopos, spaceChannel, cancel := c.topoServer.WatchSpaces(c.masterCtx)
	if err != nil {
		log.Debug("WatchDBs err[%v]", err)
		return nil
	}
	c.cancelSpaceWatch = cancel
	log.Debug("current dbs[%v] before WatchDBs", currentSpaceTopos)

	go func() {
		for space := range spaceChannel {
			if space.Err != nil && space.Err != topo.ErrNoNode {
				log.Error("watch err[%v]", space.Err)
				return
			}
			log.Debug("watched space[%v]", space.Space)
			db := c.DbCache.FindDbById(space.DB)
			if db != nil {
				oldSpace := db.SpaceCache.FindSpaceById(space.ID)
				if oldSpace == nil {
					db.SpaceCache.AddSpace(NewSpaceByMeta(space.SpaceTopo))
				} else {
					oldSpace.Update(space.SpaceTopo)
				}
			}
		}
	}()

	return currentSpaceTopos
}

func (c *Cluster) watchPartitionChange() []*topo.PartitionTopo {
	err, currentPartitionTopos, partitionChannel, _ := c.topoServer.WatchPartitions(c.masterCtx)
	if err != nil {
		log.Debug("WatchDBs err[%v]", err)
		return nil
	}
	log.Debug("current dbs[%v] before WatchDBs", currentPartitionTopos)

	go func() {
		for partition := range partitionChannel {
			if partition.Err != nil {
				if partition.Err == topo.ErrNoNode {
					c.PartitionCache.DelPartition(partition.ID)
					continue
				}
				log.Error("watch err[%v]", partition.Err)
				return
			}
			log.Debug("watched partition[%v]", partition.Partition)
			oldPartition := c.PartitionCache.FindPartitionById(partition.ID)
			if oldPartition == nil {
				c.PartitionCache.AddPartition(NewPartitionByMeta(partition.PartitionTopo))
			} else {
				oldPartition.Update(partition.PartitionTopo)
			}
		}
	}()

	return currentPartitionTopos
}

func (c *Cluster) recoveryPSCache() error {
	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	servers, err := c.PsCache.Recovery(c.config.ClusterCfg.ZoneID, c.topoServer, &c.config.PsCfg)
	if err != nil {
		return err
	}

	for _, server := range servers {
		c.PsCache.AddServer(server)
	}

	return nil
}

func (c *Cluster) recoveryDBCache() error {
	DBs := c.watchDBChange()
	if DBs == nil {
		return errors.New("watchDBChange() failed")
	}

	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	for _, db := range DBs {
		c.DbCache.AddDb(NewDBByMeta(c, db))
	}

	return nil
}

func (c *Cluster) recoverySpaceCache() error {
	allSpaces := c.WatchSpaceChange()
	if allSpaces != nil {
		return errors.New("WatchSpaceChange() failed!")
	}

	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	for _, space := range allSpaces {
		db := c.DbCache.FindDbById(space.DB)
		if db == nil {
			log.Warn("Cannot find db for the space[%v] when recovery space. discord it", space)
			continue
		}
		db.SpaceCache.AddSpace(NewSpaceByMeta(space))
	}

	return nil
}

func (c *Cluster) recoveryPartitionCache() error {
	partitions := c.watchPartitionChange()
	if partitions == nil {
		return errors.New("watchPartitionChange() failed")
	}

	c.clusterLock.Lock()
	defer c.clusterLock.Unlock()

	for _, metaPartition := range partitions {
		db := c.DbCache.FindDbById(metaPartition.DB)
		if db == nil {
			log.Warn("Cannot find db for the partition[%v] when recovery partition. discord it", metaPartition)
			continue
		}

		partition := NewPartitionByMeta(metaPartition)
		space := db.SpaceCache.FindSpaceById(partition.Space)
		if space == nil {
			log.Warn("Cannot find space for the partition[%v] when recovery partition. discord it", partition)
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
		if err := partition.deleteReplica(c.topoServer, delMetaReplicas...); err != nil {
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
