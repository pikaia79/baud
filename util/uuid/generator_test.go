package uuid

import (
	"runtime"
	"sync"
	"testing"
)

func TestFlakeUUID(t *testing.T) {
	var m sync.Map
	var wg sync.WaitGroup

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 5000000; j++ {
				uuid := FlakeUUID()
				if _, ok := m.LoadOrStore(uuid, true); ok {
					t.Fatal("FlakeUUID occur conflict.")
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
