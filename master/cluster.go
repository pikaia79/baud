package master

import (
	"util/log"
)

type Cluster struct {
	config			*Config
	metaInfo 		MSInfo

	store 			*RaftStore
}

func NewCluster() *Cluster {
	return new(Cluster)
}

func (c *Cluster) Start(config *Config) (err error) {
	c.config = config

	store := NewRaftStore(config)
	if err = store.Start(); err != nil {
		log.Error("fail to create raft store. err:[%v]", err)
		return
	}
	c.store = store


	return nil
}
func (c *Cluster) Close() {
	if c.store != nil {
		if err := c.store.Close(); err != nil {
			log.Error("fail to close raft store")
		}
	}
}

