package gm

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
)

const (
	PREFIX_DB = "scheme db "
)

type DB struct {
	*metapb.DB

	SpaceCache   *SpaceCache  `json:"-"`
	propertyLock sync.RWMutex `json:"-"`
}

func NewDB(dbName string) (*DB, error) {
	dbId, err := GetIdGeneratorSingle().GenID()
	if err != nil {
		log.Error("generate id of db[%v] is failed. err[%v]", dbName, err)
		return nil, ErrGenIdFailed
	}

	metaDb := &metapb.DB{
		ID:   metapb.DBID(dbId),
		Name: dbName,
	}
	return NewDBByMeta(metaDb), nil
}

func NewDBByMeta(metaDb *metapb.DB) *DB {
	return &DB{
		DB:         metaDb,
		SpaceCache: NewSpaceCache(),
	}
}

func (db *DB) persistent(store Store) error {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()

	dbVal, err := proto.Marshal(db.DB)
	if err != nil {
		log.Error("fail to marshal db[%v]. err:[%v]", db.DB, err)
		return err
	}

	dbKey := []byte(fmt.Sprintf("%s%d", PREFIX_DB, db.ID))
	if err := store.Put(dbKey, dbVal); err != nil {
		log.Error("fail to put db[%v] into store. err:[%v]", db.DB, err)
		return ErrLocalDbOpsFailed
	}

	return nil
}

func (db *DB) erase(store Store) error {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()

	dbKey := []byte(fmt.Sprintf("%s%d", PREFIX_DB, db.DB.ID))
	if err := store.Delete(dbKey); err != nil {
		log.Error("fail to delete db[%v] from store. err:[%v]", db.DB, err)
		return ErrLocalDbOpsFailed
	}

	return nil
}

func (db *DB) rename(newDbName string) {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()

	db.Name = newDbName
}
