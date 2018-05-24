package topo

import (
    "context"
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/tiglabs/baudengine/util/log"
    "github.com/golang/protobuf/proto"
    "path"
)

func (s *TopoServer) GetAllZones(ctx context.Context) ([]*metapb.Zone, error) {
    dirs, err := s.backend.ListDir(ctx, GlobalZone, zonesPath)
    if err != nil {
       return nil, err
    }
    if dirs == nil || len(dirs) == 0 {
        return nil, nil
    }

    zoneMetas := make([]*metapb.Zone, 0, len(dirs))
    for _, dir := range dirs {
        contents, _, err := s.backend.Get(ctx, GlobalZone, path.Join(zonesPath, dir, ZoneInfoFile))
        if err != nil {
            log.Error("Fail to get zone[%s] info from dir. err[%v]", dir, err)
            return nil, err
        }

        zoneMeta := &metapb.Zone{}
        if err := proto.Unmarshal(contents, zoneMeta); err != nil {
            log.Error("Fail to unmarshal meta info for zone[%s]. err[%v]", dir, err)
            return nil, err
        }

        zoneMetas = append(zoneMetas, zoneMeta)
    }

    return zoneMetas, nil
}

func (s *TopoServer) GetZone(ctx context.Context, zoneName string) (*metapb.Zone, error) {
    contents, _, err := s.backend.Get(ctx, GlobalZone, path.Join(zonesPath, zoneName, ZoneInfoFile))
    if err != nil {
        return nil, err
    }

    zoneMeta := &metapb.Zone{}
    if err := proto.Unmarshal(contents, zoneMeta); err != nil {
        log.Error("Fail to unmarshal meta info for zone[%s]. err[%v]", zoneName, err)
        return nil, err
    }

    return zoneMeta, nil
}

func (s *TopoServer) AddZone(ctx context.Context, zone *metapb.Zone) error {
    contents, err := proto.Marshal(zone)
    if err != nil {
        log.Error("Fail to marshal zone[%v] info. err[%v]", zone, err)
        return err
    }

    _, err = s.backend.Create(ctx, GlobalZone, path.Join(zonesPath, zone.Name, ZoneInfoFile), contents)
    if err != nil {
        return err
    }

    return nil
}

func (s *TopoServer) DeleteZone(ctx context.Context, zoneName string) error {
    return s.backend.Delete(ctx, GlobalZone, path.Join(zonesPath, zoneName, ZoneInfoFile), nil)
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