package gm

import (
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"runtime/debug"
	"sync"
	"time"
)

//go:generate mockgen -destination work_mock.go -package gm github.com/tiglabs/baudengine/gm SpaceStateTransitionWorker

type WorkerManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	workers     map[string]Worker
	workersLock sync.RWMutex
	wg          sync.WaitGroup

	cluster *Cluster
}

func NewWorkerManager(cluster *Cluster) *WorkerManager {
	wm := &WorkerManager{
		workers: make(map[string]Worker),
		cluster: cluster,
	}
	wm.ctx, wm.cancel = context.WithCancel(context.Background())

	return wm
}

func (wm *WorkerManager) Start() error {
	wm.addWorker(NewSpaceStateTransitionWorker(wm.cluster))

	wm.workersLock.RLock()
	defer wm.workersLock.RUnlock()

	for _, worker := range wm.workers {
		wm.runWorker(worker)
	}

	log.Info("Worker manager has started")
	return nil
}

func (wm *WorkerManager) Close() {
	wm.cancel()
	wm.wg.Wait()

	wm.workersLock.RLock()
	defer wm.workersLock.RUnlock()

	wm.workers = nil

	log.Info("Worker manager has closed")
}

func (wm *WorkerManager) addWorker(worker Worker) {
	if worker == nil {
		return
	}

	wm.workersLock.Lock()
	defer wm.workersLock.Unlock()

	if _, ok := wm.workers[worker.getName()]; ok {
		log.Error("worker[%v] have already existed in worker manager.", worker.getName())
		return
	}

	wm.workers[worker.getName()] = worker
}

func (wm *WorkerManager) runWorker(worker Worker) {
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()

		timer := time.NewTimer(worker.getInterval())
		defer timer.Stop()

		for {
			select {
			case <-wm.ctx.Done():
				return
			case <-timer.C:
				func() {
					log.Info("worker[%v] is running.", worker.getName())
					defer func() {
						if e := recover(); e != nil {
							log.Error("recover worker panic. e[%s] \nstack:[%s]", e, debug.Stack())
						}
					}()
					worker.run()
				}()
				timer.Reset(worker.getInterval())
			}
		}
	}()
}

type Worker interface {
	getName() string
	getInterval() time.Duration
	run()
}

type SpaceStateTransitionWorker struct {
	cluster *Cluster
}

func NewSpaceStateTransitionWorker(cluster *Cluster) *SpaceStateTransitionWorker {
	return &SpaceStateTransitionWorker{
		cluster: cluster,
	}
}

func (w *SpaceStateTransitionWorker) getName() string {
	return "Space-State-Transition-Worker"
}

func (w *SpaceStateTransitionWorker) getInterval() time.Duration {
	return time.Second * 60
}

func (w *SpaceStateTransitionWorker) run() {
	zonesMap, err := w.cluster.GetAllZonesMap()
	if err != nil {
		log.Error("GetAllZonesMap error, err:[%v]", err)
		return
	}
	zonesName, err := w.cluster.GetAllZonesName()
	if err != nil {
		log.Error("GetAllZonesName error, err:[%v]", err)
		return
	}
	partitionInfosCacheMap := make(map[metapb.PartitionID]*masterpb.PartitionInfo)

	// Get latest partitionInfo from different zones
	for _, zoneMap := range zonesMap {
		partitionIdsInZone, err := getPartitionIdsByZone(zoneMap.Name)
		if err != nil {
			log.Error("getPartitionIdsByZone error, err:[%v]", err)
			continue
		}
		for _, partitionIdInZone := range partitionIdsInZone {
			partitionInfo, err := getPartitionInfoByZone(zoneMap.Name, partitionIdInZone)
			if err != nil {
				log.Error("getPartitionInfoByZone error, err:[%v]", err)
				continue
			}
			if partitionInfo == nil {
				continue
			}
			partitionInfoInCacheMap := partitionInfosCacheMap[partitionInfo.ID]
			if partitionInfoInCacheMap == nil {
				partitionInfosCacheMap[partitionInfo.ID] = partitionInfo
				continue
			}
			if partitionInfo.Epoch.ConfVersion > partitionInfoInCacheMap.Epoch.ConfVersion {
				partitionInfosCacheMap[partitionInfo.ID] = partitionInfo
				continue
			} else if partitionInfo.Epoch.ConfVersion == partitionInfoInCacheMap.Epoch.ConfVersion {
				if partitionInfo.RaftStatus.Term > partitionInfoInCacheMap.RaftStatus.Term {
					partitionInfosCacheMap[partitionInfo.ID] = partitionInfo
					continue
				}
			}
		}
	}

	for partitionId, partitionInfoInCacheMap := range partitionInfosCacheMap {
		replicaLeaderMeta := pickLeaderReplica(partitionInfoInCacheMap)
		if replicaLeaderMeta == nil {
			continue
		}
		partition := w.cluster.PartitionCache.FindPartitionById(partitionId)
		if partition != nil {
			_, err := partition.updateReplicaGroup(partitionInfoInCacheMap)
			if err != nil {
				log.Error("updateReplicaGroup error, err:[%v]", err)
				continue
			}
		}
		// Compensation
		err := handleCompensation(partitionInfoInCacheMap, zonesName, w.cluster)
		if err != nil {
			log.Error("handleCompensation error, err:[%v]", err)
			continue
		}
	}

	dbs := w.cluster.DbCache.GetAllDBs()
	for _, db := range dbs {
		spaces := db.SpaceCache.GetAllSpaces()
		for _, space := range spaces {
			func() {
				space.propertyLock.Lock()
				defer space.propertyLock.Unlock()

				partitionsMap := space.partitions

				if space.Status == metapb.SS_Init {
					err := handleSpaceStateSSInit(db, space, partitionsMap, zonesName, w.cluster)
					if err != nil {
						log.Error("handleSpaceStateSSInit error, err:[%v]", err)
						return
					}
				} else if space.Status == metapb.SS_Running {
					err := handleSpaceStateSSRunning(db, space, partitionsMap, zonesName)
					if err != nil {
						log.Error("handleSpaceStateSSRunning error, err:[%v]", err)
						return
					}
				} else if space.Status == metapb.SS_Deleting {
					err := handleSpaceStateSSDeleting(db, space, partitionsMap, zonesName, w.cluster)
					if err != nil {
						log.Error("handleSpaceStateSSDeleting error, err:[%v]", err)
						return
					}
				}
			}()
		}
	}
}

func handleCompensation(partitionInfo *masterpb.PartitionInfo, zonesName []string, cluster *Cluster) error {
	partitionInCluster := cluster.PartitionCache.FindPartitionById(partitionInfo.ID)
	log.Info("partition id[%v], confVerPartitionInfo[%v], confVerPartitionInCluster[%v]", partitionInCluster.ID, partitionInfo.Epoch.ConfVersion, partitionInCluster.Epoch.ConfVersion)
	if partitionInfo.Epoch.ConfVersion < partitionInCluster.Epoch.ConfVersion {
		// TODO add ump告警
		return nil
	} else if partitionInfo.Epoch.ConfVersion == partitionInCluster.Epoch.ConfVersion {
		if partitionInCluster.ReplicaLeader != nil && partitionInfo.RaftStatus.Replica.ID == partitionInCluster.ReplicaLeader.ID {
			return nil
		}
		if partitionInCluster.ReplicaLeader != nil && partitionInfo.RaftStatus.Replica.ID != partitionInCluster.ReplicaLeader.ID {
			if partitionInfo.RaftStatus.Term < partitionInCluster.Term {
				// TODO add ump告警
				return nil
			}
		}
	}
	return nil
}

func handleSpaceStateSSInit(db *DB, space *Space, partitionsMap map[metapb.PartitionID]*Partition, zonesName []string, cluster *Cluster) error {
	var isSpaceReady = true
	if partitionsMap == nil || len(partitionsMap) == 0 {
		log.Error("space has no partition, db:[%s], space:[%s]", db.Name, space.Name)
		return nil
	}
	for _, partition := range partitionsMap {
		if partition.countReplicas() < FIXED_REPLICA_NUM {
			isSpaceReady = false
			replicaZoneName := NewZoneSelector().SelectTarget(zonesName)
			replicaZoneAddr, replicaLeaderZoneAddr, err := getReplicaZoneAddrAndReplicaLeaderZoneAddrForCreate(replicaZoneName, partition, cluster)
			if err != nil {
				log.Error("getReplicaZoneAddrAndRelicaLeaderZoneAddr error, err:[%v]", err)
				continue
			}
			isGrabed, err := partition.grabPartitionTaskLock(topo.GlobalZone, "partition", string(partition.ID))
			if err != nil {
				log.Error("partition grab Partition Task error, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
				continue
			}
			if !isGrabed {
				log.Info("partition has task now, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
				continue
			}
			if err := GetPMSingle(cluster).PushEvent(NewPartitionCreateEvent(replicaZoneAddr, replicaZoneName, replicaLeaderZoneAddr, partition)); err != nil {
				log.Error("fail to push event for creating partition[%v].", partition)
				continue
			}
		}
	}
	if isSpaceReady {
		space.Status = metapb.SS_Running
		space.update()
	}
	return nil
}

func handleSpaceStateSSRunning(db *DB, space *Space, partitionsMap map[metapb.PartitionID]*Partition, zonesName []string) error {
	if partitionsMap == nil || len(partitionsMap) == 0 {
		log.Error("space has no partition, db:[%s], space:[%s]", db.Name, space.Name)
		return nil
	}
	for _, partition := range partitionsMap {
		if partition.countReplicas() != FIXED_REPLICA_NUM {
			if partition.countReplicas() < FIXED_REPLICA_NUM {
				// TODO add UMP告警
			} else if partition.countReplicas() > FIXED_REPLICA_NUM {
				// TODO add UMP告警
			}
		}
	}
	return nil
}

func handleSpaceStateSSDeleting(db *DB, space *Space, partitionsMap map[metapb.PartitionID]*Partition, zonesName []string, cluster *Cluster) error {
	var isSpaceCanDelete = true
	if partitionsMap == nil || len(partitionsMap) == 0 {
		log.Error("space has no partition, db:[%s], space:[%s]", db.Name, space.Name)
		return nil
	}
	for _, partition := range partitionsMap {
		if partition.countReplicas() > 0 {
			isSpaceCanDelete = false
			replicaZoneAddr, replica, replicaLeaderZoneAddr, err := getReplicaZoneAddrAndReplicaLeaderZoneAddrForDelete(partition, nil, nil, cluster)
			if err != nil {
				log.Error("getReplicaZoneAddrAndRelicaLeaderZoneAddr error, err:[%v]", err)
				continue
			}
			isGrabed, err := partition.grabPartitionTaskLock(topo.GlobalZone, "partition", string(partition.ID))
			if err != nil {
				log.Error("partition grab Partition Task error, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
				continue
			}
			if !isGrabed {
				log.Info("partition has task now, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
				continue
			}
			if err := GetPMSingle(cluster).PushEvent(NewPartitionDeleteEvent(replicaZoneAddr, replicaLeaderZoneAddr, partition.ID, replica)); err != nil {
				log.Error("fail to push event for deleting partition[%v].", partition)
				continue
			}
		}
	}
	if isSpaceCanDelete {
		space.Status = metapb.SS_Delete
		space.erase()
	}
	return nil
}

func getReplicaZoneAddrAndReplicaLeaderZoneAddrForCreate(replicaZoneName string, partition *Partition, cluster *Cluster) (string, string, error) {
	var replicaLeaderZone string
	if partition.ReplicaLeader != nil {
		replicaLeaderZone = partition.ReplicaLeader.Zone
	} else {
		replicaLeaderZone = replicaZoneName
	}

	replicaZoneAddr, err := getZMLeaderAddr(replicaZoneName, cluster.config.ClusterCfg.GmNodeId)
	if err != nil {
		log.Error("getZMLeaderAddr() replicaZoneAddr error. err:[%v]", err)
		return "", "", err
	}
	if replicaZoneAddr == "" {
		log.Info("getZMLeaderAddr() replicaZoneAddr has no leader now.")
		return "", "", ErrNoMSLeader
	}

	replicaLeaderZoneAddr, err := getZMLeaderAddr(replicaLeaderZone, cluster.config.ClusterCfg.GmNodeId)
	if err != nil {
		log.Error("getZMLeaderAddr() replicaLeaderZoneAddr error. err:[%v]", err)
		return "", "", err
	}
	if replicaLeaderZoneAddr == "" {
		log.Info("getZMLeaderAddr() replicaLeaderZoneAddr has no leader now.")
		return "", "", ErrNoMSLeader
	}
	return replicaZoneAddr, replicaLeaderZoneAddr, nil
}

func getReplicaZoneAddrAndReplicaLeaderZoneAddrForDelete(partition *Partition, partitionInfo *masterpb.PartitionInfo, replicaToDelete *metapb.Replica, cluster *Cluster) (string, *metapb.Replica, string, error) {
	var replicaLeaderZone string
	var replica *metapb.Replica
	if partition != nil {
		log.Debug("getAddr by partition")
		if partition.ReplicaLeader == nil {
			log.Info("partition has no leader now.")
			return "", nil, "", ErrNoMSLeader
		}
		replicaLeaderZone = partition.ReplicaLeader.Zone
		if replicaToDelete == nil {
			replica = partition.pickReplicaToDelete()
		} else {
			replica = replicaToDelete
		}

	}
	if partitionInfo != nil {
		log.Debug("getAddr by partitionInfo")
		if !partitionInfo.IsLeader {
			log.Info("partitionInfo has no leader now.")
			return "", nil, "", ErrNoMSLeader
		}
		replicaLeaderZone = partitionInfo.RaftStatus.Replica.Zone
		if replicaToDelete == nil {
			replica = pickReplicaToDelete(partitionInfo)
		} else {
			replica = replicaToDelete
		}
	}

	replicaZoneName := replica.Zone
	replicaZoneAddr, err := getZMLeaderAddr(replicaZoneName, cluster.config.ClusterCfg.GmNodeId)
	if err != nil {
		log.Error("getZMLeaderAddr() replicaZoneAddr error. err:[%v]", err)
		return "", nil, "", err
	}
	if replicaZoneAddr == "" {
		log.Info("getZMLeaderAddr() replicaZoneAddr has no leader now.")
		return "", nil, "", ErrNoMSLeader
	}

	replicaLeaderZoneAddr, err := getZMLeaderAddr(replicaLeaderZone, cluster.config.ClusterCfg.GmNodeId)
	if err != nil {
		log.Error("getZMLeaderAddr() replicaLeaderZoneAddr error. err:[%v]", err)
		return "", nil, "", err
	}
	if replicaLeaderZoneAddr == "" {
		log.Info("getZMLeaderAddr() replicaLeaderZoneAddr has no leader now.")
		return "", nil, "", ErrNoMSLeader
	}
	return replicaZoneAddr, replica, replicaLeaderZoneAddr, nil
}

func getZMLeaderAddr(zoneName, id string) (string, error) {
	replicaZoneParticipation, err := TopoServer.NewMasterParticipation(zoneName, id)
	if err != nil {
		log.Error("TopoServer NewMasterParticipation error. err:[%v]", err)
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()
	replicaZoneAddr, err := replicaZoneParticipation.GetCurrentMasterID(ctx)
	if err != nil {
		log.Error("replicaZoneParticipation GetCurrentMasterID error. err:[%v]", err)
		return "", err
	}
	return replicaZoneAddr, nil
}
