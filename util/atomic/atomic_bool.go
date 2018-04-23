package atomic

import "sync/atomic"

type AtomicBool struct {
	v int32
}

func NewAtomicBool(v bool) *AtomicBool {
	return &AtomicBool{v: boolToInt(v)}
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&b.v) != 0
}

func (b *AtomicBool) Set(newValue bool) {
	atomic.StoreInt32(&b.v, boolToInt(newValue))
}

func (b *AtomicBool) CompareAndSet(expect, update bool) bool {
	return atomic.CompareAndSwapInt32(&b.v, boolToInt(expect), boolToInt(update))
}

func boolToInt(v bool) int32 {
	if v {
		return 1
	} else {
		return 0
	}
}
