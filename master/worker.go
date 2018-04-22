package master

import (
	"context"
	"sync"
	"time"
)

type WorkerManager struct {
	ctx    context.Context
	cancel context.CancelFunc

	workers *sync.Map
}

func NewWorkerManager() *WorkerManager {
	wm := &WorkerManager{
		workers: new(sync.Map),
	}
	wm.ctx, wm.cancel = context.WithCancel(context.Background())

	return wm
}

type Worker interface {
	getName() string
	getInterval() time.Duration
	run()
	stop()
}

//func NewWorker(string name, interval )
