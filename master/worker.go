package master

import (
	"context"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/util/log"
	"sync"
	"time"
)

type WorkerManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	workers *sync.Map
	wg      sync.WaitGroup

	cluster *Cluster
}

func NewWorkerManager() *WorkerManager {
	wm := &WorkerManager{
		workers: new(sync.Map),
	}
	wm.ctx, wm.cancel = context.WithCancel(context.Background())

	return wm
}

func (wm *WorkerManager) Start() error {
	wm.addWorker(NewSpaceStateTransitionWorker(wm.cluster))

	wm.workers.Range(func(name, worker interface{}) bool {
		wm.runWorker(worker.(Worker))
		return true
	})

	return nil
}

func (wm *WorkerManager) Shutdown() {
	wm.cancel()
	wm.wg.Wait()
}

func (wm *WorkerManager) addWorker(worker Worker) {
	if _, loaded := wm.workers.LoadOrStore(worker.getName(), worker); loaded {
		log.Error("worker[%v] have already added in worker manager.", worker.getName())
		return
	}
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
				log.Debug("worker[%v] is running.", worker.getName())
				worker.run()
				timer.Reset(worker.getInterval())
			}
		}
	}()
}

type Worker interface {
	getName() string
	getInterval() time.Duration
	run()
	stop()
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
	return "space_state_transition_worker"
}

func (w *SpaceStateTransitionWorker) getInterval() time.Duration {
	return time.Second * 60
}

func (w *SpaceStateTransitionWorker) run() {
	dbs := w.cluster.dbCache.getAllDBs()
	for _, db := range dbs {
		spaces := db.spaceCache.getAllSpaces()
		for _, space := range spaces {

			space.propertyLock.Lock()
			if space.Status == metapb.SS_Init {

				var zeroReplicaFound = false

				p := &Partition{
					Partition: &metapb.Partition{
						StartSlot: 0,
					},
				}
				for {
					items := space.searchTree.ascendScan(p, 100)
					if items == nil || len(items) == 0 {
						break
					}

					for i := 0; i < len(items); i++ {
						itemP := items[i].partition
						if len(itemP.Replicas) == 0 {
							zeroReplicaFound = true

							if err := PushProcessorEvent(NewPartitionCreateEvent(itemP)); err != nil {
								log.Error("fail to push event for creating partition[%v].", itemP)
							}
						}
					}

					if len(items) < 100 {
						break
					}
					p = items[len(items)-1].partition
				}

				if !zeroReplicaFound {
					space.Status = metapb.SS_Running
				}
			}
			space.propertyLock.Unlock()
		}
	}
}

func (w *SpaceStateTransitionWorker) stop() {

}
