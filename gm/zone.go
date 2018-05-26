package gm

import (
	"github.com/gogo/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"golang.org/x/net/context"
)

type Zone struct {
	*topo.ZoneTopo

	// name
	// type "global/local"
	// global etcd addr
	// global master addr
	// zone etcd addr
	// zone master addr

	propertyLock sync.RWMutex
}

func NewZone(zoneName, zoneEtcdAddr, zoneMasterAddr string) (*Zone, error) {
	metaZone := &metapb.Zone{
		Name: zoneName,
		// etcdAddr: zoneEtcdAddr,
		// zoneMasterAddr: zoneMasterAddr
	}
	topoZone := &topo.ZoneTopo{
		Zone: metaZone,

	}
	return NewZoneByTopo(topoZone), nil
}

func NewZoneByTopo(topoZone *topo.ZoneTopo) *Zone {
	return &Zone{
		ZoneTopo: topoZone,
	}
}

func (zone *Zone) add() error {
	zone.propertyLock.Lock()
	defer zone.propertyLock.Unlock()
	ctx := context.Background()

	zoneTopo, err:= topoServer.AddZone(ctx, zone.ZoneTopo.Zone)
	if err != nil {
		log.Error("topoServer AddZone error, err: [%v]", err)
		return err
	}
	zone.ZoneTopo = zoneTopo

	return nil
}

func (zone *Zone) erase() error {
	zone.propertyLock.Lock()
	defer zone.propertyLock.Unlock()

	ctx := context.Background()

	err := topoServer.DeleteZone(ctx, zone.ZoneTopo)
	if err != nil {
		log.Error("topoServer DeleteZone error, err: [%v]", err)
		return err
	}

	return nil
}

type ZoneCache struct {
	lock  sync.RWMutex
	zones map[string]*Zone
}

func NewZoneCache() *ZoneCache {
	return &ZoneCache{
		zones: make(map[string]*Zone),
	}
}

func (c *ZoneCache) FindZoneByName(zoneName string) *Zone {
	c.lock.RLock()
	defer c.lock.RUnlock()

	zone, ok := c.zones[zoneName]
	if !ok {
		return nil
	}

	return zone
}

func (c *ZoneCache) AddZone(zone *Zone) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.zones[zone.Name] = zone
}

func (c *ZoneCache) DeleteZone(zone *Zone) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.zones, zone.Name)
}

func (c *ZoneCache) GetAllZones() []*Zone {
	c.lock.RLock()
	defer c.lock.RUnlock()

	zones := make([]*Zone, 0, len(c.zones))
	for _, zone := range c.zones {
		zones = append(zones, zone)
	}

	return zones
}

func (c *ZoneCache) Recovery() ([]*Zone, error) {

	resultZones := make([]*Zone, 0)

	// TODO 调用global etcd获得zone list, 接口由@杨洋提供
	topoZones := make([]*metapb.Zone, 0)
	for _, topoZone := range topoZones {
		err := proto.Unmarshal([]byte{}, topoZone)
		if err != nil {
			log.Error("proto.Unmarshal error, err:[%v]", err)
		}
		metaZone := new(metapb.Zone)
		metaZone.Name = topoZone.Name
		metaZone.ServerAddrs = topoZone.ServerAddrs
		metaZone.RootDir = topoZone.RootDir
		// TODO global etcd, zone etcd, global addr, zone addr

		resultZones = append(resultZones, NewZoneByMeta(metaZone))
	}
	return resultZones, nil
}

func (c *ZoneCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.zones = make(map[string]*Zone)
}
