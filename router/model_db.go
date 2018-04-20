package router

import (
	"github.com/tiglabs/baud/proto/metapb"
	"sync"
)

type DB struct {
	meta metapb.DB
	masterClient *MasterClient
	spaceMap map[string]*Space
	lock     sync.RWMutex
}

func (db *DB) GetSpace(spaceName string) *Space {
	db.lock.RLock()
	defer db.lock.RUnlock()

	space, ok := db.spaceMap[spaceName]
	if ok {
		return space
	}
	return nil
}