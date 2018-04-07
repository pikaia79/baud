package master

import (
	"util/log"
)

type Cluster struct {
	config 			*Config
	store 			Store

	replGroups 		[]ReplGroup
	dbCache 		*DBCache
}

func NewCluster(config *Config) *Cluster {
	return &Cluster{
		config: 		config,
		store: 			NewRaftStore(config),
		dbCache: 		new(DBCache),
	}
}

func (c *Cluster) Start() (err error) {
	if err = c.store.Open(); err != nil {
		log.Error("fail to create raft store. err:[%v]", err)
		return
	}

	IdGeneratorSingleInstance(c.store)

	return nil
}

func (c *Cluster) Close() {
	if c.store != nil {
		c.store.Close()
	}
}

func (c *Cluster) createDb(dbName string) (*DB, error) {
	c.dbCache.lock.Lock()
	defer c.dbCache.lock.Unlock()

	db := c.dbCache.findDbByName(dbName)
	if db != nil {
		return nil, ErrDupDb
	}

	db, err := NewDB(dbName)
	if err != nil {
		return nil, err
	}

	if err := db.persistent(c.store); err != nil {
		return nil, err
	}
	c.dbCache.addDb(db)

	return db, nil
}

func (c *Cluster) renameDb(srcDbName, destDbName string) error {
	c.dbCache.lock.Lock()
	defer c.dbCache.lock.Unlock()

	srcDb := c.dbCache.findDbByName(srcDbName)
	if srcDb == nil {
		return ErrDbNotExists
	}
	destDb := c.dbCache.findDbByName(destDbName)
	if destDb != nil {
		return ErrDupDb
	}

	c.dbCache.deleteDb(srcDb)
	srcDb.rename(destDbName)
	if err := srcDb.persistent(c.store); err != nil {
		return err
	}
	c.dbCache.addDb(srcDb)

	return nil
}

func (c *Cluster) createSpace(dbName, spaceName, partitionKey, partitionFunc string, partitionNum int) (*Space, error) {
	c.dbCache.lock.RLock()
	db := c.dbCache.findDbByName(dbName)
	if db == nil {
		c.dbCache.lock.RUnlock()
		return nil, ErrDbNotExists
	}
	c.dbCache.lock.RUnlock()
	// my be hold out of db from memory, but will clear it when master startup

	spaceCache := db.spaceCache
	spaceCache.lock.Lock()
	defer spaceCache.lock.Unlock()
	if space := db.spaceCache.findSpaceByName(spaceName); space != nil {
		return nil, ErrDupSpace
	}

	space, err := NewSpace(spaceName, partitionKey, partitionFunc, partitionNum)
	if err != nil {
		return nil, err
	}

	if err := space.persistent(c.store); err != nil {
		return nil, err
	}
	spaceCache.addSpace(space)

	return space, nil
}

func (c *Cluster) renameSpace(dbName, srcSpaceName, destSpaceName string) error {
	c.dbCache.lock.RLock()
	db := c.dbCache.findDbByName(dbName)
	if db == nil {
		c.dbCache.lock.RUnlock()
		return ErrDbNotExists
	}
	c.dbCache.lock.RUnlock()
	// my be hold out of db from memory, but will clear it when master startup

	spaceCache := db.spaceCache
	spaceCache.lock.Lock()
	defer spaceCache.lock.Unlock()
	srcSpace := spaceCache.findSpaceByName(srcSpaceName)
	if srcSpace == nil {
		return ErrSpaceNotExists
	}
	destSpace := spaceCache.findSpaceByName(destSpaceName)
	if destSpace != nil {
		return ErrDupSpace
	}

	spaceCache.deleteSpace(srcSpace)
	srcSpace.rename(destSpaceName)
	if err := srcSpace.persistent(c.store); err != nil {
		return err
	}
	spaceCache.addSpace(srcSpace)

	return nil
}
