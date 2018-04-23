package atomic

import "sync/atomic"

type AtomicInt64 struct {
	v int64
}

func NewAtomicInt64(v int64) *AtomicInt64 {
	return &AtomicInt64{v: v}
}

func (a *AtomicInt64) Get() int64 {
	return atomic.LoadInt64(&a.v)
}

func (a *AtomicInt64) Set(v int64) {
	atomic.StoreInt64(&a.v, v)
}

func (a *AtomicInt64) Add(v int64) int64 {
	return atomic.AddInt64(&a.v, v)
}

func (a *AtomicInt64) Incr() int64 {
	return atomic.AddInt64(&a.v, 1)
}

func (a *AtomicInt64) Decr() int64 {
	return atomic.AddInt64(&a.v, -1)
}

func (a *AtomicInt64) CompareAndSwap(o, n int64) bool {
	return atomic.CompareAndSwapInt64(&a.v, o, n)
}
