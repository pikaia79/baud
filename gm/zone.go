package gm

import (
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"sync"
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

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zoneTopo, err := topoServer.AddZone(ctx, zone.ZoneTopo.Zone)
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

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

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

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zonesTopo, err := topoServer.GetAllZones(ctx)
	if err != nil {
		log.Error("topoServer GetAllZones error, err: [%v]", err)
		return nil, err
	}
	if zonesTopo != nil {
		for _, zoneTopo := range zonesTopo {
			zone := &Zone{
				ZoneTopo: zoneTopo,
			}
			resultZones = append(resultZones, zone)
		}
	}

	return resultZones, nil
}

func (c *ZoneCache) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.zones = make(map[string]*Zone)
}
