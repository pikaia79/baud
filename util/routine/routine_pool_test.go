package routine

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	GoWork(func() { wg.Done() })
	wg.Wait()
}

func TestRoutineGC(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		GoWork(func() {
			time.Sleep(time.Millisecond)
			wg.Done()
		})
	}
	wg.Wait()
	time.Sleep(2 * time.Minute)

	if atomic.LoadInt64(&count) != 0 {
		t.Errorf("%d routines not be recycled", atomic.LoadInt64(&count))
	}
}

func BenchmarkPool(b *testing.B) {
	b.Run("Pool.emptyFn", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				GoWork(dummy)
			}
		})
	})
	b.Run("Pool.emptyFn.two", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				GoWork(dummy)
			}
		})
	})

	b.Run("Pool.stackFn", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var wg sync.WaitGroup
				wg.Add(1)
				GoWork(func() {
					stack(true)
					wg.Done()
				})
				wg.Wait()
			}
		})
	})
	b.Run("Pool.stackFn.two", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var wg sync.WaitGroup
				wg.Add(1)
				GoWork(func() {
					stack(true)
					wg.Done()
				})
				wg.Wait()
			}
		})
	})
}

func BenchmarkGo(b *testing.B) {
	b.Run("Go.emptyFn", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				go dummy()
			}
		})
	})
	b.Run("Go.stackFn", func(b *testing.B) {
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					stack(true)
					wg.Done()
				}()
				wg.Wait()
			}
		})
	})
}

func dummy() {
}

func stack(f bool) {
	var stack [8 * 1024]byte
	if f {
		for i := 0; i < len(stack); i++ {
			stack[i] = 'a'
		}
	}
}
