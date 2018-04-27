package master

import (
	"fmt"
	"github.com/tiglabs/baud/util/log"
	"sync"
	"sync/atomic"
	"github.com/tiglabs/baud/util"
)

var (
	GEN_STEP          uint32 = 10
	AUTO_INCREMENT_ID        = fmt.Sprintf("$auto_increment_id")

	idGeneratorSingle     IDGenerator
	idGeneratorSingleLock sync.Once
)

type IDGenerator interface {
	GenID() (uint32, error)
}

func GetIdGeneratorSingle(store Store) IDGenerator {
	idGeneratorSingleLock.Do(func() {
		if store == nil {
			log.Error("store should not be nil at first time when create single idGenerator")
			return
		}
		idGeneratorSingle = NewIDGenerator([]byte(AUTO_INCREMENT_ID), GEN_STEP, store)
	})
	return idGeneratorSingle
}

type idGenerator struct {
	lock sync.Mutex
	base uint32
	end  uint32

	key  []byte
	step uint32

	// TODO: put operation of this store transfer new end value yielded through raft message,
	// may be slow GenID()
	store Store
}

func NewIDGenerator(key []byte, step uint32, store Store) *idGenerator {
	return &idGenerator{key: key, step: step, store: store}
}

func (id *idGenerator) GenID() (uint32, error) {
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

func (id *idGenerator) get(key []byte) ([]byte, error) {
	value, err := id.store.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (id *idGenerator) put(key, value []byte) error {
	return id.store.Put(key, value)
}

func (id *idGenerator) generate() (uint32, error) {
	value, err := id.get(id.key)
	if err != nil {
		return 0, err
	}

	if value == nil || len(value) != 4 {
		log.Error("invalid data, must 4 bytes, but %d", len(value))
		return 0, ErrInternalError
	}

	end := util.BytesToUint32(value)
	end += id.step
	value = util.Uint32ToBytes(end)
	err = id.put(id.key, value)
	if err != nil {
		return 0, err
	}

	return end, nil
}
