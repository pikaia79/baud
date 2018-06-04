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
