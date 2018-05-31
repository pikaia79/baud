package zm

import (
	"context"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"time"
)

const (
	DEFAULT_REPLICA_LIMIT_PER_PS = 1
	PREFIX_PARTITION_SERVER      = "schema ps "
)

type PSStatus int32

const (
	PS_INVALID PSStatus = iota
	PS_INIT
	PS_REGISTERED
	PS_OFFLINE
	PS_TOMBSTONE
	PS_LOGOUT
)

type PartitionServer struct {
	*topo.PsTopo
	*masterpb.NodeSysStats

	adminPort      uint32
	status         PSStatus
	lastHeartbeat  time.Time
	partitionCache *PartitionCache
	propertyLock   sync.RWMutex
}

func NewPartitionServer(ip string, psCfg *PsConfig) (*PartitionServer, error) {
	newId, err := GetIdGeneratorSingle(nil).GenID()
	if err != nil {
		log.Error("fail to allocate ps id. err[%v]", err)
		return nil, ErrGenIdFailed
	}

	metaNode := &metapb.Node{
		ID: metapb.NodeID(newId),
		Ip: ip,
		ReplicaAddrs: metapb.ReplicaAddrs{
			HeartbeatAddr: util.BuildAddr(ip, psCfg.RaftHeartbeatPort),
			ReplicateAddr: util.BuildAddr(ip, psCfg.RaftReplicatePort),
			RpcAddr:       util.BuildAddr(ip, psCfg.RpcPort),
			AdminAddr:     util.BuildAddr(ip, psCfg.AdminPort),
		},
	}
	return NewPartitionServerByMeta(psCfg, &topo.PsTopo{Node: metaNode}), nil
}

func NewPartitionServerByMeta(psCfg *PsConfig, metaPS *topo.PsTopo) *PartitionServer {
	return &PartitionServer{
		PsTopo:         metaPS,
		NodeSysStats:   new(masterpb.NodeSysStats),
		status:         PS_INIT,
		adminPort:      psCfg.AdminPort,
		lastHeartbeat:  time.Now(),
		partitionCache: NewPartitionCache(),
	}
}

func (p *PartitionServer) persistent(zone string, topoServer *topo.TopoServer) error {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), TOPO_TIMEOUT)
	defer cancel()

	psTopo, err := topoServer.AddPsByZone(ctx, zone, p.Node)
	if err != nil {
		log.Error("fail to store ps into store. err[%v]", err)
		return ErrLocalDbOpsFailed
	}
	p.PsTopo = psTopo

	return nil
}

func (p *PartitionServer) addPartition(partition *Partition) {
	if partition == nil {
		return
	}

	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	p.partitionCache.AddPartition(partition)
}

func (p *PartitionServer) updateHb() {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	p.lastHeartbeat = time.Now()
}

func (p *PartitionServer) changeStatus(newStatus PSStatus) {
	p.propertyLock.Lock()
	defer p.propertyLock.Unlock()

	oldStatus := p.status

	var isConfusing bool
	switch newStatus {
	case PS_INIT:
		if oldStatus != PS_INIT {
			isConfusing = true
		}
	case PS_OFFLINE:
	case PS_LOGOUT:
	default:
		log.Error("can not change to the new ps Status[%v]", newStatus)
		return
	}

	if !isConfusing {
		p.status = newStatus
	} else {
		log.Error("can not change ps[%v] Status from [%v] to [%v]", p.ID, oldStatus, newStatus)
	}
}

func (p *PartitionServer) getRpcAddr() string {
	p.propertyLock.RLock()
	defer p.propertyLock.RUnlock()

	return util.BuildAddr(p.Ip, p.adminPort)
}

type PSCache struct {
	lock       sync.RWMutex
	id2Servers map[metapb.NodeID]*PartitionServer
	ip2Servers map[string]*PartitionServer
}

func NewPSCache() *PSCache {
	return &PSCache{
		id2Servers: make(map[metapb.NodeID]*PartitionServer),
		ip2Servers: make(map[string]*PartitionServer),
	}
}

func (c *PSCache) GetAllServers() []*PartitionServer {
	c.lock.RLock()
	defer c.lock.RUnlock()

	servers := make([]*PartitionServer, 0, len(c.id2Servers))
	for _, ps := range c.id2Servers {
		servers = append(servers, ps)
	}

	return servers
}

func (c *PSCache) FindServerByAddr(addr string) *PartitionServer {
	if len(addr) == 0 {
		return nil
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	ps, ok := c.ip2Servers[addr]
	if !ok {
		return nil
	}
	return ps
}

func (c *PSCache) FindServerById(psId metapb.NodeID) *PartitionServer {
	if psId == 0 {
		return nil
	}

	c.lock.RLock()
	defer c.lock.RUnlock()

	ps, ok := c.id2Servers[psId]
	if !ok {
		return nil
	}
	return ps
}

func (c *PSCache) AddServer(server *PartitionServer) {
	if server == nil {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	c.id2Servers[server.ID] = server
	c.ip2Servers[server.Ip] = server
}

func (c *PSCache) Recovery(zone string, topoServer *topo.TopoServer, psCfg *PsConfig) ([]*PartitionServer, error) {
	ctx, cancel := context.WithTimeout(context.Background(), TOPO_TIMEOUT)
	defer cancel()

	partitionServers, err := topoServer.GetAllPsByZone(ctx, zone)
	if err != nil {
		log.Error("topoServer.GetAllPsByZone() failed: %s", err.Error())
	}

	resultServers := make([]*PartitionServer, 0)
	for _, metaPS := range partitionServers {
		resultServers = append(resultServers, NewPartitionServerByMeta(psCfg, metaPS))
	}

	return resultServers, nil
}

func (c *PSCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.id2Servers = make(map[metapb.NodeID]*PartitionServer)
	c.ip2Servers = make(map[string]*PartitionServer)
}
