package topo

import (
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/tiglabs/baudengine/proto/masterpb"
    "github.com/golang/protobuf/proto"
    "context"
    "path"
    "fmt"
    "github.com/tiglabs/baudengine/util/log"
)

type PartitionTopo struct {
    version Version
    *metapb.Partition
}

type PartitionInfoTopo struct {
    version Version
    *masterpb.PartitionInfo
}

func (s *TopoServer) GetAllPartition(ctx context.Context) ([]*PartitionTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    partitionIds, err := s.backend.ListDir(ctx, GlobalZone, partitionsPath)
    if err != nil {
        return nil, err
    }
    if partitionIds == nil || len(partitionIds) == 0 {
        return nil, nil
    }

    partitions := make([]*PartitionTopo, 0, len(partitionIds))
    for _, partitionId := range partitionIds {
        contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(partitionsPath, fmt.Sprint(partitionId),
                PartitionTopoFile))
        if err != nil {
            return nil, err
        }

        partitionMeta := &metapb.Partition{}
        if err := proto.Unmarshal(contents, partitionMeta); err != nil {
            log.Error("Fail to unmarshal meta data for partition[%d]. err[%v]", partitionId, err)
            return nil, err
        }

        partition := &PartitionTopo{version:version, Partition: partitionMeta}
        partitions = append(partitions, partition)
    }

    return partitions, nil
}

func (s *TopoServer) GetPartition(ctx context.Context, partitionId metapb.PartitionID) (*PartitionTopo, error) {
    if ctx == nil {
        return nil, ErrNoNode
    }

    contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(partitionsPath, fmt.Sprint(partitionId),
            PartitionTopoFile))
    if err != nil {
        return nil, err
    }

    partitionMeta := &metapb.Partition{}
    if err := proto.Unmarshal(contents, partitionMeta); err != nil {
        log.Error("Fail to unmarshal meta data for partition[%d]. err[%v]", partitionId, err)
        return nil, err
    }

    partition := &PartitionTopo{version:version, Partition: partitionMeta}
    return partition, nil
}

func (s *TopoServer) UpdatePartition(ctx context.Context, partition *PartitionTopo) error {
    if ctx == nil || partition == nil {
        return ErrNoNode
    }

    contents, err := proto.Marshal(partition.Partition)
    if err != nil {
        log.Error("Fail to marshal meta data for partition[%v]. err[%v]", partition, err)
        return err
    }

    newVersion, err := s.backend.Update(ctx, GlobalZone,
            path.Join(partitionsPath, fmt.Sprint(partition.ID), PartitionTopoFile), contents, partition.version)
    if err != nil {
        return err
    }
    partition.version = newVersion

    return nil
}

func (s *TopoServer) DeletePartition(ctx context.Context, partition *PartitionTopo) error {
    if ctx == nil || partition == nil {
        return ErrNoNode
    }

    return s.backend.Delete(ctx, GlobalZone, path.Join(partitionsPath, fmt.Sprint(partition.ID), PartitionTopoFile),
            partition.version)
}
