package master

import (
    "sync"
    "proto/metapb"
    "time"
    "util"
    "util/log"
    "proto/masterpb"
    "util/deepcopy"
    "fmt"
    "github.com/gogo/protobuf/proto"
)

const (
    DEFAULT_REPLICA_LIMIT_PER_PS = 1
    PREFIX_PARTITION_SERVER    	  = "schema ps "
)

type PSStatus   int32

const (
    PS_Invalid PSStatus = iota
    PS_Init
    PS_Registered
    PS_OFFLINE
    PS_TOMBSTONE
    PS_LOGOUT
)

type PartitionServer struct {
    *metapb.Node
    *masterpb.NodeSysStats

    status PSStatus
    lastHeartbeat   time.Time
    partitionCache  *PartitionCache  // this partition only belong to same ps
    propertyLock    sync.RWMutex
}

func NewPartitionServer(ip string) (*PartitionServer, error) {
    newId, err := GetIdGeneratorInstance(nil).GenID()
    if err != nil {
        log.Error("fail to generate ps id. err[%v]", err)
        return nil, ErrGenIdFailed
    }

    return &PartitionServer{
        Node: &metapb.Node{
            ID: newId,
            Ip: ip,
        },
        NodeSysStats:   new(masterpb.NodeSysStats),
        status:         PS_Init,
        lastHeartbeat:  time.Now(),
        partitionCache: new(PartitionCache),
    }, nil
}

func (p *PartitionServer) persistent(store Store) error {
    p.propertyLock.RLock()
    defer p.propertyLock.RUnlock()

    psCopy := deepcopy.Iface(p.Node).(*metapb.Node)
    psVal, err := proto.Marshal(psCopy)
    if err != nil {
        log.Error("marshal ps[%v] is failed.", psCopy)
        return ErrInternalError
    }

    psKey := []byte(fmt.Sprintf("%s%d", PREFIX_PARTITION_SERVER, psCopy.ID))
    if err := store.Put(psKey, psVal); err != nil {
        log.Error("fail to store partition into store. err[%v]", err)
        return ErrBoltDbOpsFailed
    }

    return nil
}

func (p *PartitionServer) isReplicaFull() bool {
    p.propertyLock.RLock()
    defer p.propertyLock.RUnlock()

    return p.replicaCache.count() == DEFAULT_REPLICA_LIMIT_PER_PS
}

func (p *PartitionServer) changeStatus(newStatus metapb.PSStatus) {
    p.propertyLock.Lock()
    defer p.propertyLock.Unlock()

    oldStatus := p.status

    var isConfusing bool
    switch newStatus {
    case metapb.PSStatus_PS_Login:
        if oldStatus != metapb.PSStatus_PS_Initial {
            isConfusing = true
        }
    case metapb.PSStatus_PS_Offline:
    case metapb.PSStatus_PS_Logout:
    default:
        log.Error("can not change to the new ps Status[%v]", newStatus)
        return
    }

    if !isConfusing {
        p.status = newStatus
    } else {
        log.Error("can not change ps[%v] Status from [%v] to [%v]", p.Id, oldStatus, newStatus)
    }
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

func (c *PSCache) findServerById(psId uint32) *PartitionServer {
    c.lock.RLock()
    defer c.lock.RUnlock()

    ps, ok := c.id2Servers[psId]
    if !ok {
        return nil
    }
    return ps
}

func (c *PSCache) addServer(server *PartitionServer) {
    c.lock.Lock()
    defer c.lock.Unlock()


    c.id2Servers[server.ID] = server
    c.addr2Servers[util.BuildAddr(server.Ip, server.Port)] = server
}


