package master

import (
	"fmt"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"sync/atomic"
)

var (
	GEN_STEP          uint32 = 10
	AUTO_INCREMENT_ID        = fmt.Sprintf("$auto_increment_id")

	idGeneratorSingle     IDGenerator
	idGeneratorSingleLock sync.Mutex
	idGeneratorSingleDone uint32
)

type IDGenerator interface {
	GenID() (uint32, error)
	Close()
}

func GetIdGeneratorSingle(store Store) IDGenerator {
	if idGeneratorSingle != nil {
		return idGeneratorSingle
	}
	if atomic.LoadUint32(&idGeneratorSingleDone) == 1 {
		return idGeneratorSingle
	}

	idGeneratorSingleLock.Lock()
	defer idGeneratorSingleLock.Unlock()

	if atomic.LoadUint32(&idGeneratorSingleDone) == 0 {
		if store == nil {
			log.Error("store should not be nil at first time when create psClientSingle StoreIdGenerator")
			return nil
		}
		idGeneratorSingle = NewIDGenerator([]byte(AUTO_INCREMENT_ID), GEN_STEP, store)
		atomic.StoreUint32(&idGeneratorSingleDone, 1)

		log.Info("IdGenerator single has started")
	}

	return idGeneratorSingle
}

type StoreIdGenerator struct {
	lock sync.Mutex
	base uint32
	end  uint32

	key  []byte
	step uint32

	// TODO: put operation of this store transfer new end value yielded through raft message,
	// may be slow GenID()
	store Store
}

func NewIDGenerator(key []byte, step uint32, store Store) *StoreIdGenerator {
	return &StoreIdGenerator{key: key, step: step, store: store}
}

func (id *StoreIdGenerator) GenID() (uint32, error) {
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

	atomic.AddUint32(&(id.base), 1)

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

func (id *StoreIdGenerator) get(key []byte) ([]byte, error) {
	value, err := id.store.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (id *StoreIdGenerator) put(key, value []byte) error {
	return id.store.Put(key, value)
}

func (id *StoreIdGenerator) generate() (uint32, error) {
	value, err := id.get(id.key)
	if err != nil {
		return 0, err
	}

	if value != nil && len(value) != 4 {
		log.Error("invalid data, must 4 bytes, but %d", len(value))
		return 0, ErrInternalError
	}

    var end uint32
	if len(value) != 0 {
        end = util.BytesToUint32(value)
    }
	end += id.step
	value = util.Uint32ToBytes(end)
	err = id.put(id.key, value)
	if err != nil {
		return 0, err
	}

	return end, nil
}
