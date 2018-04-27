package router

import (
	"context"
	"github.com/tiglabs/baudengine/proto/metapb"
	"sync"
)

type DB struct {
	meta         metapb.DB
	masterClient *MasterClient
	spaceMap     sync.Map
	context      context.Context
}

func NewDB(masterClient *MasterClient, meta metapb.DB) *DB {
	return &DB{meta: meta, masterClient: masterClient}
}

func (db *DB) GetSpace(spaceName string) *Space {
	space, ok := db.spaceMap.Load(spaceName)
	if !ok {
		spaceMeta := db.masterClient.GetSpace(db.meta.ID, spaceName)
		space, ok = db.spaceMap.LoadOrStore(spaceMeta.Name, NewSpace(db, spaceMeta))
	}
	return space.(*Space)
}
