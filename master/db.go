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

	spaceCache   *SpaceCache        `json:"-"`
	propertyLock sync.RWMutex           `json:"-"`
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
	db.propertyLock.RLock()
	defer db.propertyLock.RUnlock()

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
	db.propertyLock.RLock()
	defer db.propertyLock.RUnlock()
	
	dbKey := []byte(fmt.Sprintf("%s %d", PREFIX_DB, db.DB.Id))
	if err := store.Delete(dbKey); err != nil {
		log.Error("fail to delete db[%v] from store. err:[%v]", db.DB, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (db *DB) rename(newDbName string) {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()

	db.Name = newDbName
}

type DBCache struct {
	lock      sync.RWMutex
	dbs       map[string]*DB
	idNameMap map[uint32]string
}

func (c *DBCache) findDbByName(dbName string) *DB {
	c.lock.RLock()
	defer c.lock.RUnlock()

	db, ok := c.dbs[dbName]
	if !ok {
		return nil
	}
	return db
}

func (c *DBCache) addDb(db *DB) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.dbs[db.Name] = db
	c.idNameMap[db.Id] = db.Name
}

func (c *DBCache) deleteDb(db *DB) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.dbs, db.Name)
	delete(c.idNameMap, db.Id)
}