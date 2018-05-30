package gm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"runtime/debug"
	"sync"
	"time"
)

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
	// TODO 从各个zone etcd里获得partitions的信息, 和partitiotn replica leader id
	//1) zone/<zone name>/partitions/<partition id>/group/psinfo_info
	//2) zone/<zone name>/partitions/<partition id>/leader/<replica id>
	//3) <zone name>/servers/<ps id>/partitions/<partition id>/replicas/<replica id>/replica_info       data:  metapb.Replica

	dbs := w.cluster.DbCache.GetAllDBs()
	for _, db := range dbs {
		spaces := db.SpaceCache.GetAllSpaces()
		for _, space := range spaces {
			func() {
				space.propertyLock.Lock()
				defer space.propertyLock.Unlock()
				partitionsMap := space.partitions

				if space.Status == metapb.SS_Init {
					var isSpaceReady = true
					if partitionsMap == nil || len(partitionsMap) == 0 {
						log.Error("space has no partition, db:[%s], space:[%s]", db.Name, space.Name)
						return
					}
					for _, partition := range partitionsMap {
						if partition.countReplicas() < FIXED_REPLICA_NUM {
							isSpaceReady = false
							isGrabed, err := partition.grabPartitionTaskLock(topo.GlobalZone, "partition", string(partition.ID))
							if err != nil {
								log.Error("partition grab Partition Task error, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
								continue
							}
							if !isGrabed {
								log.Info("partition has task now, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
								continue
							}
							if err := GetPMSingle(w.cluster).PushEvent(NewPartitionCreateEvent(partition)); err != nil {
								log.Error("fail to push event for creating partition[%v].", partition)
							}
						}
					}
					if isSpaceReady {
						space.Status = metapb.SS_Running
						space.update()
					}
				} else if space.Status == metapb.SS_Running {
					if partitionsMap == nil || len(partitionsMap) == 0 {
						log.Error("space has no partition, db:[%s], space:[%s]", db.Name, space.Name)
						return
					}
					for _, partition := range partitionsMap {
						if partition.countReplicas() != FIXED_REPLICA_NUM {
							isGrabed, err := partition.grabPartitionTaskLock(topo.GlobalZone, "partition", string(partition.ID))
							if err != nil {
								log.Error("partition grab Partition Task error, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
								continue
							}
							if !isGrabed {
								log.Info("partition has task now, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
								continue
							}
							if partition.countReplicas() < FIXED_REPLICA_NUM {
								if err := GetPMSingle(w.cluster).PushEvent(NewPartitionCreateEvent(partition)); err != nil {
									log.Error("fail to push event for creating partition[%v].", partition)
								}
							} else {
								// TODO 1. get zone master rpc, 2. sure the replica which will delete
								if err := GetPMSingle(w.cluster).PushEvent(NewPartitionDeleteEvent("", partition.ID, nil)); err != nil {
									log.Error("fail to push event for creating partition[%v].", partition)
								}
							}
						}
					}
				} else if space.Status == metapb.SS_Deleting {
					var isSpaceCanDelete = true
					if partitionsMap == nil || len(partitionsMap) == 0 {
						log.Error("space has no partition, db:[%s], space:[%s]", db.Name, space.Name)
						return
					}
					for _, partition := range partitionsMap {
						if partition.countReplicas() > 0 {
							isSpaceCanDelete = false
							isGrabed, err := partition.grabPartitionTaskLock(topo.GlobalZone, "partition", string(partition.ID))
							if err != nil {
								log.Error("partition grab Partition Task error, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
								continue
							}
							if !isGrabed {
								log.Info("partition has task now, db:[%s], space:[%s], partition:[%d]", db.Name, space.Name, partition.ID)
								continue
							}
							// TODO 1. get zone master rpc, 2. sure the replica which will delete
							if err := GetPMSingle(w.cluster).PushEvent(NewPartitionDeleteEvent("", partition.ID, nil)); err != nil {
								log.Error("fail to push event for creating partition[%v].", partition)
							}
						}
					}
					if isSpaceCanDelete {
						space.Status = metapb.SS_Delete
						space.erase()
					}
				}
			}()
		}
	}
}
