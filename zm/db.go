package zm

import (
	"context"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
)

const (
	PREFIX_DB = "scheme db "
)

type DB struct {
	*topo.DBTopo
	parent       *Cluster
	SpaceCache   *SpaceCache  `json:"-"`
	propertyLock sync.RWMutex `json:"-"`
	timeWheel    TimeWheel
}

func (db *DB) Update(dbTopo *topo.DBTopo) {
	db.propertyLock.Lock()
	defer db.propertyLock.Unlock()

	db.DBTopo = dbTopo
}

func NewDBByMeta(parent *Cluster, metaDb *topo.DBTopo) *DB {
	return &DB{
		DBTopo:     metaDb,
		parent:     parent,
		SpaceCache: NewSpaceCache(),
	}
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

func (c *DBCache) Recovery(cluster *Cluster) ([]*DB, error) {
	ctx, cancel := context.WithTimeout(cluster.masterCtx, TOPO_TIMEOUT)
	defer cancel()

	DBs, err := cluster.topoServer.GetAllDBs(ctx)
	if err != nil {
		log.Error("topoServer.GetAllDBs() failed: %s", err.Error())
	}

	resultDBs := make([]*DB, 0)
	for _, metaDb := range DBs {
		resultDBs = append(resultDBs, NewDBByMeta(cluster, metaDb))
	}

	return resultDBs, nil
}

func (c *DBCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.dbs = make(map[metapb.DBID]*DB)
	c.name2Ids = make(map[string]metapb.DBID)
}
