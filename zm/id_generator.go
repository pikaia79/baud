package zm

import (
	"context"
	"fmt"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"sync/atomic"
)

var (
	GEN_STEP          uint32 = 100
	AUTO_INCREMENT_ID        = fmt.Sprintf("$auto_increment_id")

	idGeneratorSingle     IDGenerator
	idGeneratorSingleLock sync.Mutex
	idGeneratorSingleDone uint32
)

type IDGenerator interface {
	GenID() (uint64, error)
	Close()
}

func GetIdGeneratorSingle(topoServer *topo.TopoServer) IDGenerator {
	if idGeneratorSingle != nil {
		return idGeneratorSingle
	}
	if atomic.LoadUint32(&idGeneratorSingleDone) == 1 {
		return idGeneratorSingle
	}

	idGeneratorSingleLock.Lock()
	defer idGeneratorSingleLock.Unlock()

	if atomic.LoadUint32(&idGeneratorSingleDone) == 0 {
		if topoServer == nil {
			log.Error("topoServer should not be nil at first time when create IdGenerator single")
			return nil
		}
		idGeneratorSingle = NewIDGenerator(GEN_STEP, topoServer)
		atomic.StoreUint32(&idGeneratorSingleDone, 1)

		log.Info("IdGenerator single has started")
	}

	return idGeneratorSingle
}

type StoreIdGenerator struct {
	lock sync.Mutex
	base uint64
	end  uint64

	step uint32

	// TODO: put operation of this store transfer new end value yielded through raft message,
	// may be slow GenID()
	topoServer *topo.TopoServer
}

func NewIDGenerator(step uint32, topoServer *topo.TopoServer) *StoreIdGenerator {
	return &StoreIdGenerator{step: step, topoServer: topoServer}
}

func (id *StoreIdGenerator) GenID() (uint64, error) {
	if id == nil {
		return 0, ErrInternalError
	}

	if id.base == id.end {
		id.lock.Lock()

		if id.base == id.end {
			log.Debug("[GENID] before allocate!!!!!! (base %d, end %d)", id.base, id.end)
			ctx, cancel := context.WithTimeout(context.Background(), TOPO_TIMEOUT)
			defer cancel()
			start, end, err := id.topoServer.GenerateNewId(ctx, uint64(id.step))
			if err != nil {
				id.lock.Unlock()
				return 0, err
			}

			id.base = start
			id.end = end
			log.Debug("[GENID] after allocate!!!!!! (base %d, end %d)", id.base, id.end)
		}

		id.lock.Unlock()
	}

	atomic.AddUint64(&(id.base), 1)

	return id.base, nil
}

func (id *StoreIdGenerator) Close() {
	if id == nil {
		return
	}

	idGeneratorSingleLock.Lock()
	defer idGeneratorSingleLock.Unlock()

	idGeneratorSingle = nil
	atomic.StoreUint32(&idGeneratorSingleDone, 0)

	log.Info("IdGenerator single has closed")
}
