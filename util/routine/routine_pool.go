package routine

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	idle int32 = 0
	use  int32 = 1
	dead int32 = 2
)

var (
	count    int64
	capacity = runtime.NumCPU()
	mask     = int64(capacity - 1)
	poolMap  = make([]*pool, capacity)
)

func init() {
	for i := 0; i < capacity; i++ {
		poolMap[i] = newPool()
	}
}

// GoWork execute the f function in a routine,
// The routine is reused.
func GoWork(f func()) {
	idx := atomic.AddInt64(&count, 1)
	poolMap[idx&mask].goWork(f)
}

type pool struct {
	sync.Mutex
	head routine
	tail *routine

	idleDuration time.Duration
}

func newPool() *pool {
	p := &pool{
		idleDuration: 10 * time.Minute,
	}
	p.tail = &p.head
	return p
}

func (p *pool) goWork(f func()) {
	for {
		r := p.get()
		if atomic.CompareAndSwapInt32(&r.status, idle, use) {
			r.queue <- f
			return
		}
	}
}

func (p *pool) get() *routine {
	p.Lock()
	if p.head.next == nil {
		p.Unlock()
		return p.createRoutine()
	}

	head := &p.head
	ret := head.next
	head.next = ret.next
	if ret == p.tail {
		p.tail = head
	}
	p.Unlock()

	ret.next = nil
	return ret
}

func (p *pool) createRoutine() *routine {
	r := &routine{
		queue: make(chan func()),
	}
	go r.routineLoop(p)
	return r
}

type routine struct {
	queue  chan func()
	next   *routine
	status int32
}

func (r *routine) put(pool *pool) {
	atomic.StoreInt32(&r.status, idle)

	pool.Lock()
	pool.tail.next = r
	pool.tail = r
	pool.Unlock()

	atomic.AddInt64(&count, -1)
}

func (r *routine) routineLoop(pool *pool) {
	timer := time.NewTimer(pool.idleDuration)
	for {
		select {
		case work := <-r.queue:
			work()
			r.put(pool)

		case <-timer.C:
			if ok := atomic.CompareAndSwapInt32(&r.status, idle, dead); ok {
				return
			}
		}

		timer.Reset(pool.idleDuration)
	}
}
