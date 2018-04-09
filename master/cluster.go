package master

import (
	"util/log"
)

type Cluster struct {
	replGroups []ReplGroup

	config *Config

	store   Store
	idGenor *IDGenerator
}

func NewCluster(config *Config) *Cluster {
	return &Cluster{
		config: config,
		store:  NewRaftStore(config),
	}
}

func (c *Cluster) Start() (err error) {
	if err = c.store.Open(); err != nil {
		log.Error("fail to create raft store. err:[%v]", err)
		return
	}

	return nil
}

func (c *Cluster) Close() {
	if c.store != nil {
		c.store.Close()
	}
}

func (c *Cluster) createDb(dbName string) (*DB, error) {
	return nil, nil
}
