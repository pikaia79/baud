package master

import (
	"sync"
	"fmt"
	"util/log"
	"proto/metapb"
	"util/deepcopy"
	"util"
	"github.com/gogo/protobuf/proto"
)

const (
	PREFIX_DB 		 = "scheme db "
)

type DB struct {
	*metapb.DB

	spaceCache   *SpaceCache        `json:"-"`
	propertyLock sync.RWMutex           `json:"-"`
}

func NewDB(dbName string) (*DB, error) {
	dbId, err := GetIdGeneratorInstance(nil).GenID()
	if err != nil {
		log.Error("generate db id is failed. err[%v]", err)
		return nil, ErrGenIdFailed
	}
	db := &DB{
		DB:		&metapb.DB{
			ID: 	dbId,
			Name: 	dbName,
		},
		spaceCache: 	new(SpaceCache),
	}
	return db, nil
}

func NewDBByMeta(metaDb *metapb.DB) *DB {
	return &DB{
		DB:         metaDb,
		spaceCache: new(SpaceCache),
	}
}

func (db *DB) persistent(store Store) error {
	db.propertyLock.RLock()
	defer db.propertyLock.RUnlock()

	copy := deepcopy.Iface(db.DB).(*metapb.DB)
	dbVal, err := proto.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal db[%v]. err:[%v]", copy, err)
		return err
	}

	dbKey := []byte(fmt.Sprintf("%s%d", PREFIX_DB, copy.ID))
	if err := store.Put(dbKey, dbVal); err != nil {
		log.Error("fail to put db[%v] into store. err:[%v]", copy, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

func (db *DB) erase(store Store) error {
	db.propertyLock.RLock()
	defer db.propertyLock.RUnlock()
	
	dbKey := []byte(fmt.Sprintf("%s%d", PREFIX_DB, db.DB.ID))
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
	c.idNameMap[db.ID] = db.Name
}

func (c *DBCache) deleteDb(db *DB) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.dbs, db.Name)
	delete(c.idNameMap, db.ID)
}

func (c *DBCache) recovery(store Store) (*DBCache, error) {
	prefix := []byte(fmt.Sprintf("%s", PREFIX_DB))
	startKey, limitKey := util.BytesPrefix(prefix)

	var dbCache = new(DBCache)

	iterator := store.Scan(startKey, limitKey)
	defer iterator.Release()
	for iterator.Next() {
		if iterator.Key() == nil {
			continue
		}

		val := iterator.Value()
		metaDb := new(metapb.DB)
		if err := proto.Unmarshal(val, metaDb); err != nil {
			log.Error("fail to unmasharl db from store. err[%v]", err)
			continue
		}
		dbCache.addDb(NewDBByMeta(metaDb))
	}

	return dbCache, nil
}