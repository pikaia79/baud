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

	propertyLock sync.RWMutex
}

func NewZone(zoneName, zoneEtcdAddr, zoneRootDir string) (*Zone, error) {
	metaZone := &metapb.Zone{
		Name:        zoneName,
		ServerAddrs: zoneEtcdAddr,
		RootDir:     zoneRootDir,
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

	zoneTopo, err := TopoServer.AddZone(ctx, zone.ZoneTopo.Zone)
	if err != nil {
		log.Error("TopoServer AddZone error, err: [%v]", err)
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

	err := TopoServer.DeleteZone(ctx, zone.ZoneTopo)
	if err != nil {
		log.Error("TopoServer DeleteZone error, err: [%v]", err)
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

func (c *ZoneCache) GetAllZonesName() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	zonesName := make([]string, 0, len(c.zones))
	for zoneName := range c.zones {
		zonesName = append(zonesName, zoneName)
	}
	return zonesName
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

func (c *ZoneCache) GetAllZonesMap() map[string]*Zone {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.zones
}

func (c *ZoneCache) Recovery() ([]*Zone, error) {

	resultZones := make([]*Zone, 0)

	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	zonesTopo, err := TopoServer.GetAllZones(ctx)
	if err != nil {
		log.Error("TopoServer GetAllZones error, err: [%v]", err)
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
