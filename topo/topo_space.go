package topo

import (
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/golang/protobuf/proto"
    "context"
    "path"
    "github.com/tiglabs/baudengine/util/log"
    "fmt"
)

type SpaceTopo struct {
    version Version
    *metapb.Space
}

func (s *TopoServer) GetAllSpaces(ctx context.Context) ([]*SpaceTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    dbAndSpaceIds, err := s.backend.ListDir(ctx, GlobalZone, path.Join(spacesPath))
    if err != nil {
        return nil, err
    }
    if dbAndSpaceIds == nil || len(dbAndSpaceIds) == 0 {
        return nil, nil
    }

    spaceTopos := make([]*SpaceTopo, 0)
    for _, dbAndSpaceId := range dbAndSpaceIds {
        spaceTopo := &SpaceTopo{}
        contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(spacesPath, dbAndSpaceId, SpaceTopoFile))
        if err != nil {
            return nil, err
        }
        if err := proto.Unmarshal(contents, spaceTopo.Space); err != nil {
            return nil, err
        }

        spaceTopo.version = version
        spaceTopos = append(spaceTopos, spaceTopo)
    }

    return spaceTopos, nil
}

func (s *TopoServer) GetSpace(ctx context.Context, dbId metapb.DBID, spaceId metapb.SpaceID) (*SpaceTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    spaceTopo := &SpaceTopo{}
    nodePath := path.Join(spacesPath, fmt.Sprintf("%d-%d", dbId, spaceId), SpaceTopoFile)

    contents, version, err := s.backend.Get(ctx, GlobalZone, nodePath)
    if err != nil {
        return nil, err
    }
    if err := proto.Unmarshal(contents, spaceTopo.Space); err != nil {
        return nil, err
    }

    spaceTopo.version = version

    return spaceTopo, nil
}

func (s *TopoServer) AddSpace(ctx context.Context, dbId metapb.DBID, space *metapb.Space,
        partitions []*metapb.Partition) (*SpaceTopo, []*PartitionTopo, error) {
    if ctx == nil || space == nil || partitions == nil || len(partitions) == 0 {
        return nil, nil, ErrNoNode
    }

    txn, err := s.backend.NewTransaction(ctx, GlobalZone)
    if err != nil {
        log.Error("Fail to create transaction. err[%v]", err)
        return nil, nil, err
    }

    contents, err := proto.Marshal(space)
    if err != nil {
        log.Error("Fail to marshal meta data for space[%v]. err[%v]", space, err)
        return nil, nil, err
    }
    txn.Create(path.Join(spacesPath, fmt.Sprintf("%d-%d", dbId, space.ID), SpaceTopoFile), contents)

    for _, partition := range partitions {
        contents, err := proto.Marshal(partition)
        if err != nil {
            log.Error("Fail to marshal meta data for partition[%v]. err[%v]", partitions, err)
            return nil, nil, err
        }
        txn.Create(path.Join(partitionsPath, fmt.Sprint(partition.ID), PartitionTopoFile), contents)
    }

    opResults, err := txn.Commit()
    if err != nil {
        return nil, nil, err
    }
    if len(opResults) != 1+len(partitions) { // space and all partitions
        return nil, nil, ErrNoNode
    }

    spaceTopo := &SpaceTopo{version: opResults[0].(*TxnCreateOpResult).Version, Space: space}
    partitionTopos := make([]*PartitionTopo, 0)
    for i := 1; i <= len(partitions); i++ {
        partitionTopo := &PartitionTopo{version:opResults[i].(*TxnCreateOpResult).Version, Partition: partitions[i]}
        partitionTopos = append(partitionTopos, partitionTopo)
    }

    return spaceTopo, partitionTopos, nil
}

func (s *TopoServer) UpdateSpace(ctx context.Context, space *SpaceTopo) error {
    return nil
}

func (s *TopoServer) DeleteSpace(ctx context.Context, space *SpaceTopo) error {
    return nil
}
