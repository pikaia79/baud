package master

import (
    "sync"
    "time"
	"proto/metapb"
	"util/deepcopy"
	"fmt"
	"encoding/json"
	"util/log"
)

const (
	PREFIX_REPLICA   string = "schema replica"
)

type Replica struct {
	*metapb.Replica
//	ps 				*PartitionServer

    lastHeartbeat   time.Time
	propertyLock    sync.RWMutex
}

func NewReplica(replica *metapb.Replica) *Replica {
	return &Replica{
		Replica:       replica,
		lastHeartbeat: time.Now(),
	}
}

func (r *Replica) update(repl *metapb.Replica) {
	r.propertyLock.Lock()
	defer r.propertyLock.Unlock()

	r.Replica.Status = repl.GetStatus()
	r.Replica.IsLeader = repl.IsLeader
	r.lastHeartbeat = time.Now()
}

func (r *Replica) persistent(store Store) error {
	r.propertyLock.RLock()
	defer r.propertyLock.RUnlock()

	copy := deepcopy.Iface(r.Replica).(*metapb.Replica)
	replVal, err := json.Marshal(copy)
	if err != nil {
		log.Error("fail to marshal replpb[%v]. err:[%v]", copy, err)
		return err
	}
	replKey := []byte(fmt.Sprintf("%s %d", PREFIX_REPLICA, copy.Id))
	if err := store.Put(replKey, replVal); err != nil {
		log.Error("fail to put replica[%v] into store. err:[%v]", copy, err)
		return ErrBoltDbOpsFailed
	}

	return nil
}

type ReplicaCache struct {
    lock sync.RWMutex
    replicas map[uint32]*Replica
}

func (c *ReplicaCache) count() int {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return len(c.replicas)
}

func (c *ReplicaCache) getAllReplicas() []*Replica {
	c.lock.RLock()
	defer c.lock.RUnlock()

	replicas := make([]*Replica, len(c.replicas))
	for _, replica := range c.replicas {
		replicas = append(replicas, replica)
	}

	return replicas
}

func (c *ReplicaCache) addReplica(replica *Replica) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.replicas[replica.GetId()] = replica
}