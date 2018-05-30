package topo

import (
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/golang/protobuf/proto"
    "context"
    "path"
    "fmt"
    "github.com/tiglabs/baudengine/util/log"
)

type PsTopo struct {
    version Version
    *metapb.Node
}

func (s *TopoServer) GetAllPsByZone(ctx context.Context, zoneName string) ([]*PsTopo, error) {
    if ctx == nil || len(zoneName) == 0 {
        return nil, ErrNoNode
    }

    psIds, _, err := s.backend.ListDir(ctx, zoneName, partitionServersPath)
    if err != nil {
        return nil, err
    }
    if psIds == nil || len(psIds) == 0 {
        return nil, nil
    }

    servers := make([]*PsTopo, 0, len(psIds))
    for _, psId := range psIds {
        contents, version, err := s.backend.Get(ctx, zoneName,
                path.Join(partitionServersPath, fmt.Sprint(psId), PartitionServerTopoFile))
        if err != nil {
            return nil, err
        }

        psMeta := &metapb.Node{}
        if err := proto.Unmarshal(contents, psMeta); err != nil {
            log.Error("Fail to unmarshal meta data for ps[%d]. err[%v]", psId, err)
            return nil, err
        }

        ps := &PsTopo{version: version, Node: psMeta}
        servers = append(servers, ps)
    }

    return servers, nil
}

func (s *TopoServer) GetPsByZone(ctx context.Context, zoneName string, psId metapb.NodeID) (*PsTopo, error) {
    if ctx == nil || len(zoneName) == 0 {
        return nil, ErrNoNode
    }

    contents, version, err := s.backend.Get(ctx, zoneName,
        path.Join(partitionServersPath, fmt.Sprint(psId), PartitionServerTopoFile))
    if err != nil {
        return nil, err
    }

    psMeta := &metapb.Node{}
    if err := proto.Unmarshal(contents, psMeta); err != nil {
        log.Error("Fail to unmarshal meta data for ps[%d]. err[%v]", psId, err)
        return nil, err
    }

    return &PsTopo{version: version, Node: psMeta}, nil
}

func (s *TopoServer) AddPsByZone(ctx context.Context, zoneName string, node *metapb.Node) (*PsTopo, error) {
    if ctx == nil || len(zoneName) == 0 || node == nil {
        return nil, ErrNoNode
    }

    contents, err := proto.Marshal(node)
    if err != nil {
        log.Error("Fail to marshal meta data for ps[%v]. err[%v]", node, err)
        return nil, err
    }

    version, err := s.backend.Create(ctx, zoneName,
        path.Join(partitionServersPath, fmt.Sprint(node.ID), PartitionServerTopoFile), contents)
    if err != nil {
        return nil, err
    }

    return &PsTopo{version: version, Node: node}, nil
}

func (s *TopoServer) UpdatePsByZone(ctx context.Context, zoneName string, ps *PsTopo) error {
    if ctx == nil || len(zoneName) == 0 || ps == nil {
        return ErrNoNode
    }

    nodePath := path.Join(partitionServersPath, fmt.Sprint(ps.ID), PartitionServerTopoFile)
    contents, err := proto.Marshal(ps.Node)
    if err != nil {
        return err
    }

    newVersion, err := s.backend.Update(ctx, zoneName, nodePath, contents, ps.version)
    if err != nil {
        return ErrNoNode
    }

    ps.version = newVersion
    return nil
}

func (s *TopoServer) DeletePsByZone(ctx context.Context, zoneName string, ps *PsTopo) error {
    if ctx == nil || len(zoneName) == 0 || ps == nil {
        return ErrNoNode
    }

    nodePath := path.Join(partitionServersPath, fmt.Sprint(ps.ID), PartitionServerTopoFile)
    return s.backend.Delete(ctx, zoneName, nodePath, ps.version)
}