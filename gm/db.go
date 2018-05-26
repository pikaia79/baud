package gm

import (
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"golang.org/x/net/context"
)

type DB struct {
	*topo.DBTopo

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

	topoDb := &topo.DBTopo{
		DB: metaDb,
	}
	return NewDBByTopo(topoDb), nil
}

func NewDBByTopo(topoDb *topo.DBTopo) *DB {
	return &DB{
		DBTopo:         topoDb,
		SpaceCache: NewSpaceCache(),
	}
}

func (db *DB) add() error {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()
	ctx := context.Background()

	dbTopo, err := topoServer.AddDB(ctx, db.DBTopo.DB)
	if err != nil {
		log.Error("topoServer AddDB error, err: [%v]", err)
		return err
	}
	db.DBTopo = dbTopo

	return nil
}

func (db *DB) update() error {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()
	ctx := context.Background()

	err := topoServer.UpdateDB(ctx, db.DBTopo)
	if err != nil {
		log.Error("topoServer UpdateDB error, err: [%v]", err)
		return err
	}
	return nil
}

func (db *DB) erase() error {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()

	ctx := context.Background()

	err := topoServer.DeleteDB(ctx, db.DBTopo)
	if err != nil {
		log.Error("topoServer DeleteDB error, err: [%v]", err)
		return err
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

func (c *DBCache) Recovery() ([]*DB, error) {

	resultDBs := make([]*DB, 0)
	// TODO 从global etcd里获得所有DB list, 由@杨洋提供接口
	topoDBs := make([]*metapb.DB, 0)
	for _, topoDB := range topoDBs {
		err := proto.Unmarshal([]byte{}, topoDB)
		if err != nil {
			log.Error("proto.Unmarshal error, err:[%v]", err)
		}
		metaDb := new(metapb.DB)
		metaDb.Name = topoDB.Name
		metaDb.ID = topoDB.ID
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
