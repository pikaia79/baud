package gm

import (
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
	"time"
)

var (
	GEN_STEP uint64 = 100

	idGeneratorSingle     IDGenerator
	idGeneratorSingleLock sync.Mutex
	idGeneratorSingleDone uint32
)

type IDGenerator interface {
	GenID() (uint64, error)
	Close()
}

func GetIdGeneratorSingle() IDGenerator {
	if idGeneratorSingle != nil {
		return idGeneratorSingle
	}
	if atomic.LoadUint32(&idGeneratorSingleDone) == 1 {
		return idGeneratorSingle
	}

	idGeneratorSingleLock.Lock()
	defer idGeneratorSingleLock.Unlock()

	if atomic.LoadUint32(&idGeneratorSingleDone) == 0 {
		idGeneratorSingle = NewIDGenerator([]byte("$auto_id"), GEN_STEP)
		atomic.StoreUint32(&idGeneratorSingleDone, 1)

		log.Info("IdGenerator single has started")
	}

	return idGeneratorSingle
}

type IdGenerator struct {
	lock sync.Mutex
	base uint64
	end  uint64

	key  []byte
	step uint64
}

func NewIDGenerator(key []byte, step uint64) *IdGenerator {
	return &IdGenerator{key: key, step: step}
}

func (id *IdGenerator) Close() {
	if id == nil {
		return
	}

	idGeneratorSingleLock.Lock()
	defer idGeneratorSingleLock.Unlock()

	idGeneratorSingle = nil
	atomic.StoreUint32(&idGeneratorSingleDone, 0)

	log.Info("IdGenerator single has closed")
}

func (id *IdGenerator) GenID() (uint64, error) {
	if id == nil {
		return 0, ErrInternalError
	}

	if id.base == id.end {
		id.lock.Lock()

		if id.base == id.end {
			log.Debug("[GENID] before generate!!!!!! (base %d, end %d)", id.base, id.end)
			end, err := id.generate()
			if err != nil {
				id.lock.Unlock()
				return 0, err
			}

			id.end = end
			id.base = id.end - id.step
			log.Debug("[GENID] after generate!!!!!! (base %d, end %d)", id.base, id.end)
		}

		id.lock.Unlock()
	}

	atomic.AddUint64(&id.base, 1)
	return id.base, nil
}

func (id *IdGenerator) generate() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start, err := topoServer.GenerateNewId(ctx)
	if err != nil {
		return 0, err
	}
	end := start + id.step
	return end, nil
}
