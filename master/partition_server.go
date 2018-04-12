package master

import (
    "sync"
    "proto/metapb"
    "time"
    "util"
)

const (
    DEFAULT_REPLICA_LIMIT_PER_PS = 1
)

type PartitionServer struct {
    *metapb.PartitionServer
    *metapb.ServerResource            `json:"-"`

    status        metapb.PSStatus
    lastHeartbeat time.Time
    replicaCache  *ReplicaCache // those replicas belong to different partitions
    propertyLock  sync.RWMutex
}

func NewPartitionServer(server *metapb.PartitionServer, resource *metapb.ServerResource) *PartitionServer {
    return &PartitionServer{
        PartitionServer: server,
        ServerResource:  resource,
        status:          metapb.PSStatus_PS_Initial,
        lastHeartbeat:   time.Now(),
        replicaCache:    new(ReplicaCache),
    }
}

func (p *PartitionServer) isReplicaFull() bool {
    p.propertyLock.RLock()
    defer p.propertyLock.RUnlock()

    return p.replicaCache.count() == DEFAULT_REPLICA_LIMIT_PER_PS
}

type PSCache struct {
    lock         sync.RWMutex
    id2Servers   map[uint32]*PartitionServer
    addr2Servers map[string]*PartitionServer  // key is ip:port
}

func (c *PSCache) getAllServers() []*PartitionServer {
    c.lock.RLock()
    defer c.lock.RUnlock()

    servers := make([]*PartitionServer, len(c.id2Servers))
    for _, ps := range c.id2Servers {
        servers = append(servers, ps)
    }

    return servers
}

func (c *PSCache) findServerByAddr(addr string) *PartitionServer {
    c.lock.RLock()
    defer c.lock.RUnlock()

    ps, ok := c.addr2Servers[addr]
    if !ok {
        return nil
    }
    return ps
}

func (c *PSCache) addServer(server *PartitionServer) {
    c.lock.Lock()
    defer c.lock.Unlock()


    c.id2Servers[server.Id] = server
    c.addr2Servers[util.BuildAddr(server.Ip, server.Port)] = server
}


