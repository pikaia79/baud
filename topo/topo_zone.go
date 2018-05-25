package topo

import (
    "context"
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/tiglabs/baudengine/util/log"
    "github.com/golang/protobuf/proto"
    "path"
)

type ZoneTopo struct {
    version Version
    *metapb.Zone
}

func (s *TopoServer) GetAllZones(ctx context.Context) ([]*ZoneTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    names, err := s.backend.ListDir(ctx, GlobalZone, zonesPath)
    if err != nil {
       return nil, err
    }
    if names == nil || len(names) == 0 {
        return nil, nil
    }

    zones := make([]*ZoneTopo, 0, len(names))
    for _, name := range names {
        contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(zonesPath, name, ZoneTopoFile))
        if err != nil {
            log.Error("Fail to get zone[%s] info from dir. err[%v]", name, err)
            return nil, err
        }

        zoneMeta := &metapb.Zone{}
        if err := proto.Unmarshal(contents, zoneMeta); err != nil {
            log.Error("Fail to unmarshal meta data for zone[%s]. err[%v]", name, err)
            return nil, err
        }

        zone := &ZoneTopo{version: version, Zone: zoneMeta}
        zones = append(zones, zone)
    }

    return zones, nil
}

func (s *TopoServer) GetZone(ctx context.Context, zoneName string) (*ZoneTopo, error) {
    if ctx == nil || len(zoneName) == 0 {
        return nil, ErrNoNode
    }

    contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(zonesPath, zoneName, ZoneTopoFile))
    if err != nil {
        return nil, err
    }

    zoneMeta := &metapb.Zone{}
    if err := proto.Unmarshal(contents, zoneMeta); err != nil {
        log.Error("Fail to unmarshal meta data for zone[%s]. err[%v]", zoneName, err)
        return nil, err
    }

    zone := &ZoneTopo{version: version, Zone: zoneMeta}

    return zone, nil
}

func (s *TopoServer) AddZone(ctx context.Context, zone *metapb.Zone) (*ZoneTopo, error) {
    if ctx == nil || zone == nil {
        return nil, ErrNoNode
    }

    contents, err := proto.Marshal(zone)
    if err != nil {
        log.Error("Fail to marshal zone[%v] meta data. err[%v]", zone, err)
        return nil, err
    }

    version, err := s.backend.Create(ctx, GlobalZone, path.Join(zonesPath, zone.Name, ZoneTopoFile), contents)
    if err != nil {
        return nil, err
    }

    return &ZoneTopo{version: version, Zone: zone}, nil
}

func (s *TopoServer) DeleteZone(ctx context.Context, zone *ZoneTopo) error {
    if ctx == nil || zone == nil {
        return ErrNoNode
    }
    return s.backend.Delete(ctx, GlobalZone, path.Join(zonesPath, zone.Name, ZoneTopoFile), zone.version)
}


//// GetKnownCells implements topo.Server.GetKnownCells.
//func (s *Server) GetKnownCells(ctx context.Context) ([]string, error) {
//	nodePath := path.Join(s.global.root, cellsPath) + "/"
//	resp, err := s.global.cli.Get(ctx, nodePath,
//		clientv3.WithPrefix(),
//		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
//		clientv3.WithKeysOnly())
//	if err != nil {
//		return nil, convertError(err)
//	}
//
//	prefixLen := len(nodePath)
//	suffix := "/" + topo.CellInfoFile
//	suffixLen := len(suffix)
//
//	var result []string
//	for _, ev := range resp.Kvs {
//		p := string(ev.Key)
//		if strings.HasPrefix(p, nodePath) && strings.HasSuffix(p, suffix) {
//			p = p[prefixLen : len(p)-suffixLen]
//			result = append(result, p)
//		}
//	}
//
//	return result, nil
//}