package router

import (
	"context"
	"github.com/tiglabs/baud/proto/metapb"
	"sync"
)

type DB struct {
	meta         metapb.DB
	masterClient *MasterClient
	spaceMap     map[string]*Space
	context      context.Context
	lock         sync.RWMutex
}

func NewDB(masterClient *MasterClient, meta metapb.DB) *DB {
	return &DB{meta: meta, masterClient: masterClient}
}

func (db *DB) GetSpace(spaceName string) *Space {
	space := db.getSpace(spaceName)
	if space == nil {
		spaceMeta := db.masterClient.GetSpace(db.meta.ID, spaceName)
		space = db.addSpace(spaceMeta)
	}

	return space
}

func (db *DB) addSpace(space metapb.Space) *Space {
	db.lock.Lock()
	defer db.lock.Unlock()

	if newSpace, ok := db.spaceMap[space.Name]; ok {
		return newSpace
	} else {
		newSpace = NewSpace(db, space)
		db.spaceMap[space.Name] = newSpace
		return newSpace
	}
}

func (db *DB) getSpace(spaceName string) *Space {
	db.lock.RLock()
	defer db.lock.RUnlock()

	space, ok := db.spaceMap[spaceName]
	if ok {
		return space
	}
	return nil
}
