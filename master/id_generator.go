package master

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"util/log"
)

var (
	GEN_STEP          uint32 = 10
	AUTO_INCREMENT_ID string = fmt.Sprintf("$auto_increment_id")

	idGeneratorInstance IDGenerator
	idGeneratorSyncOnce sync.Once
)

type IDGenerator interface {
	GenID() (uint32, error)
}

func GetIdGeneratorInstance(store Store) IDGenerator {
	idGeneratorSyncOnce.Do(func() {
		idGeneratorInstance = NewIDGenerator([]byte(AUTO_INCREMENT_ID), GEN_STEP, store)
	})
	return idGeneratorInstance
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

	var end uint32

	if value != nil {
		end, err = bytesToUint32(value)
		if err != nil {
			return 0, err
		}
	}
	end += id.step
	value = uint32ToBytes(end)
	err = id.put(id.key, value)
	if err != nil {
		return 0, err
	}

	return end, nil
}

func bytesToUint32(b []byte) (uint32, error) {
	if len(b) != 4 {
		return 0, fmt.Errorf("invalid data, must 4 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint32(b), nil
}

func uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
