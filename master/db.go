package master

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
	dbId, err := GetIdGeneratorSingle(nil).GenID()
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

type DBCache struct {
	lock     sync.RWMutex
	dbs      map[metapb.DBID]*DB
	name2Ids map[string]metapb.DBID
}

func NewDBCache() *DBCache {
	return &DBCache{
		dbs:      make(map[metapb.DBID]*DB),
		name2Ids: make(map[string]metapb.DBID),
	}
}

func (c *DBCache) FindDbByName(dbName string) *DB {
	c.lock.RLock()
	defer c.lock.RUnlock()

	id, ok := c.name2Ids[dbName]
	if !ok {
		return nil
	}

	db, ok := c.dbs[id]
	if !ok {
		log.Error("!!!db cache map not consistent, db[%v : %v] not exists. never happened", dbName, id)
		return nil
	}
	return db
}

func (c *DBCache) FindDbById(dbId metapb.DBID) *DB {
	c.lock.RLock()
	defer c.lock.RUnlock()

	db, ok := c.dbs[dbId]
	if !ok {
		return nil
	}

	return db
}

func (c *DBCache) AddDb(db *DB) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.dbs[db.ID] = db
	c.name2Ids[db.Name] = db.ID
}

func (c *DBCache) DeleteDb(db *DB) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.dbs, db.ID)
	delete(c.name2Ids, db.Name)
}

func (c *DBCache) GetAllDBs() []*DB {
	c.lock.RLock()
	defer c.lock.RUnlock()

	dbs := make([]*DB, 0, len(c.dbs))
	for _, db := range c.dbs {
		dbs = append(dbs, db)
	}

	return dbs
}

func (c *DBCache) Recovery(store Store) ([]*DB, error) {
	prefix := []byte(PREFIX_DB)
	startKey, limitKey := util.BytesPrefix(prefix)

	resultDBs := make([]*DB, 0)

	iterator := store.Scan(startKey, limitKey)
	defer iterator.Release()
	for iterator.Next() {
		if iterator.Key() == nil {
			log.Error("db store key is nil. never happened!!!")
			continue
		}

		val := iterator.Value()
		metaDb := new(metapb.DB)
		if err := proto.Unmarshal(val, metaDb); err != nil {
			log.Error("fail to unmarshal db from store. err[%v]", err)
			return nil, ErrInternalError
		}

		resultDBs = append(resultDBs, NewDBByMeta(metaDb))
	}

	return resultDBs, nil
}

func (c *DBCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.dbs = make(map[metapb.DBID]*DB)
	c.name2Ids = make(map[string]metapb.DBID)
}
