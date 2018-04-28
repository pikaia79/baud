package uuid

import (
	"strconv"
	"sync"
	"time"
)

// timeGenerator is for tests.
type timeGenerator struct {
	sync.Mutex
	// ensure clock moves forward
	lastTimestamp int64
}

func NewTimeGenerator() Generator {
	return &timeGenerator{lastTimestamp: time.Now().UnixNano()}
}

func (g *timeGenerator) GetUUID() string {
	cur := time.Now().UnixNano()

	g.Lock()
	if cur == g.lastTimestamp {
		cur++
	}
	g.lastTimestamp = cur
	g.Unlock()

	return strconv.FormatInt(cur, 10)
}
