package master

import (
	"sync"
	"fmt"
	"github.com/gin-gonic/gin/json"
	"github.com/prometheus/common/log"
	"proto/metapb"
	"util/deepcopy"
)

const (
	PREFIX_DB 		string = "scheme sdb"
)

type DB struct {
	*metapb.DB

	spaceCache *SpaceCache        `json:"-"`
}

func NewDB(dbName string) (*DB, error) {
	dbId, err := IdGeneratorSingleInstance(nil).GenID()
	if err != nil {
		log.Error("generate db id is failed. err[%v]", err)
		return nil, ErrGenIdFailed
	}
	db := &DB{
		DB:		&metapb.DB{
			Id: 	dbId,
			Name: 	dbName,
		},
		spaceCache: 	new(SpaceCache),
	}
	return db, nil
}

func (db *DB) persistent(store Store) error {
	copy := deepcopy.Iface(db.DB).(*metapb.DB)
	dbVal, err := json.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal db[%v]. err:[%v]", copy, err)
		return err
	}

	dbKey := []byte(fmt.Sprintf("%s %d", PREFIX_DB, copy.Id))
	if err := store.Put(dbKey, dbVal); err != nil {
		log.Error("fail to put db[%v] into store. err:[%v]", copy, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (db *DB) erase(store Store) error {
	dbKey := []byte(fmt.Sprintf("%s %d", PREFIX_DB, db.DB.Id))
	if err := store.Delete(dbKey); err != nil {
		log.Error("fail to delete db[%v] from store. err:[%v]", db.DB, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (db *DB) rename(newDbName string) {
	db.Name = newDbName
}

type DBCache struct {
	lock      sync.RWMutex
	dbNameMap map[string]*DB
}

func (c *DBCache) findDbByName(dbName string) *DB {
	db, ok := c.dbNameMap[dbName]
	if !ok {
		return nil
	}
	return db
}

func (c *DBCache) addDb(db *DB) {
	c.dbNameMap[db.Name] = db
}

func (c *DBCache) deleteDb(db *DB) {
	delete(c.dbNameMap, db.Name)
}