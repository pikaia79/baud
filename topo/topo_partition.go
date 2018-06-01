package topo

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"path"
	"strconv"
	"strings"
)

type PartitionTopo struct {
	Version Version
	*metapb.Partition
}

type PartitionWatchData struct {
	*PartitionTopo
	Err error
}

type PartitionInfoTopo struct {
	Version Version
	*masterpb.PartitionInfo
}

type ZonesForPartitionWatchData struct {
	zones []string
	Err   error
}

func (s *TopoServer) GetAllPartitions(ctx context.Context) ([]*PartitionTopo, error) {
	if ctx == nil {
		return nil, ErrNoNode
	}

	partitionIds, _, err := s.backend.ListDir(ctx, GlobalZone, partitionsPath)
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

		partition := &PartitionTopo{Version: version, Partition: partitionMeta}
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

	partition := &PartitionTopo{Version: version, Partition: partitionMeta}
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
		path.Join(partitionsPath, fmt.Sprint(partition.ID), PartitionTopoFile), contents, partition.Version)
	if err != nil {
		return err
	}
	partition.Version = newVersion

	return nil
}

func (s *TopoServer) DeletePartition(ctx context.Context, partition *PartitionTopo) error {
	if ctx == nil || partition == nil {
		return ErrNoNode
	}

	return s.backend.Delete(ctx, GlobalZone, path.Join(partitionsPath, fmt.Sprint(partition.ID), PartitionTopoFile),
		partition.Version)
}

func (s *TopoServer) GetPartitionInfoByZone(ctx context.Context, zoneName string,
	partitionId metapb.PartitionID) (*masterpb.PartitionInfo, error) {
	if ctx == nil || len(zoneName) == 0 {
		return nil, ErrNoNode
	}

	nodePath := path.Join(partitionsPath, fmt.Sprint(partitionId), partitionGroupTopoFile)
	contents, _, err := s.backend.Get(ctx, zoneName, nodePath)
	if err != nil {
		return nil, err
	}

	psInfoMeta := &masterpb.PartitionInfo{}
	if err := proto.Unmarshal(contents, psInfoMeta); err != nil {
		log.Error("Fail to unmarshal meta data for partitionInfo. partitionId[%d]. err[%v]", partitionId, err)
		return nil, err
	}

	return psInfoMeta, nil
}

func (s *TopoServer) SetPartitionInfoByZone(ctx context.Context, zoneName string,
	partitionInfo *masterpb.PartitionInfo) error {
	if ctx == nil || len(zoneName) == 0 || partitionInfo == nil {
		return ErrNoNode
	}

	contents, err := proto.Marshal(partitionInfo)
	if err != nil {
		log.Error("Fail to marshal meta data for partitionInfo[%v]. err[%v]", partitionInfo, err)
		return err
	}

	nodePath := path.Join(partitionsPath, fmt.Sprint(partitionInfo.ID), partitionGroupTopoFile)
	if _, err := s.backend.Update(ctx, zoneName, nodePath, contents, nil); err != nil {
		return err
	}

	return nil
}

func (s *TopoServer) GetAllPartitionIdsByZone(ctx context.Context, zoneName string) ([]metapb.PartitionID, error) {
	if ctx == nil || len(zoneName) == 0 {
		return nil, ErrNoNode
	}

	dirPath := path.Join(partitionsPath) + "/"
	ids, _, err := s.backend.ListDir(ctx, zoneName, dirPath)
	if err != nil && err != ErrNoNode {
		return nil, err
	}
	if err == ErrNoNode || ids == nil || len(ids) == 0 {
		return nil, nil
	}

	partitionIds := make([]metapb.PartitionID, 0, len(ids))
	for _, id := range ids {
		partitionId, err := strconv.Atoi(id)
		if err != nil {
			log.Error("Invalid paritionId sub directory[%s]", id)
			continue
		}
		partitionIds = append(partitionIds, metapb.PartitionID(partitionId))
	}

	return partitionIds, nil
}

func (s *TopoServer) GetPartitionsOnPsByZone(ctx context.Context, zoneName string,
	psId metapb.NodeID) ([]*PartitionTopo, error) {
	if ctx == nil || len(zoneName) == 0 {
		return nil, ErrNoNode
	}

	parentPath := path.Join(partitionServersPath, fmt.Sprint(psId), partitionsPath)
	dirs, _, err := s.backend.ListDir(ctx, zoneName, parentPath)
	if err != nil {
		return nil, err
	}
	if dirs == nil || len(dirs) == 0 {
		return nil, nil
	}

	partitions := make([]*PartitionTopo, 0, len(dirs))
	for _, id := range dirs {
		childPath := path.Join(parentPath, id, PartitionTopoFile)
		contents, version, err := s.backend.Get(ctx, zoneName, childPath)
		if err != nil {
			return nil, err
		}

		partitionMeta := &metapb.Partition{}
		if err := proto.Unmarshal(contents, partitionMeta); err != nil {
			log.Error("Fail to unmarshal meta info for partition[%s]. err[%v]", id, err)
			return nil, err
		}

		partitions = append(partitions, &PartitionTopo{Version: version, Partition: partitionMeta})
	}

	return partitions, nil
}

func (s *TopoServer) SetPartitionsOnPSByZone(ctx context.Context, zoneName string, psId metapb.NodeID,
	partitions []*metapb.Partition) error {
	if ctx == nil || len(zoneName) == 0 || len(partitions) == 0 {
		return ErrNoNode
	}

	parentPath := path.Join(partitionServersPath, fmt.Sprint(psId), partitionsPath)
	txn, err := s.backend.NewTransaction(ctx, zoneName)
	if err != nil {
		return err
	}

	txn.Delete(parentPath, nil)
	for _, partition := range partitions {
		contents, err := proto.Marshal(partition)
		if err != nil {
			return err
		}

		nodePath := path.Join(parentPath, fmt.Sprint(partition.ID), PartitionTopoFile)
		txn.Create(nodePath, contents)
	}
	if _, err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *TopoServer) SetZonesForPartition(ctx context.Context, partitionId metapb.PartitionID, zones []string) error {
	if ctx == nil || len(zones) == 0 {
		return ErrNoNode
	}

	nodePath := path.Join(partitionsPath, fmt.Sprint(partitionId), ZonesPath)
	_, err := s.backend.Update(ctx, GlobalZone, nodePath, buildZonesData(zones), nil)
	if err != nil {
		return err
	}

	return nil
}

func (s *TopoServer) GetZonesForPartition(ctx context.Context, partitionId metapb.PartitionID) ([]string, error) {
	if ctx == nil {
		return nil, ErrNoNode
	}

	nodePath := path.Join(partitionsPath, fmt.Sprint(partitionId), ZonesPath)
	contents, _, err := s.backend.Get(ctx, GlobalZone, nodePath)
	if err != nil {
		return nil, err
	}

	return parseZonesData(contents), nil
}

func buildZonesData(zones []string) []byte {
	return []byte(strings.Join(zones, "|"))
}

func parseZonesData(data []byte) []string {
	return strings.Split(string(data), "|")
}

func (s *TopoServer) WatchZonesForPartition(ctx context.Context, partitionId metapb.PartitionID) (*ZonesForPartitionWatchData, <-chan *ZonesForPartitionWatchData, CancelFunc) {
	if ctx == nil {
		return &ZonesForPartitionWatchData{Err: ErrNoNode}, nil, nil
	}

	nodePath := path.Join(partitionsPath, fmt.Sprint(partitionId), ZonesPath)
	current, wdChannel, wdCancel := s.backend.Watch(ctx, GlobalZone, nodePath)
	if current.Err != nil {
		return &ZonesForPartitionWatchData{Err: current.Err}, nil, nil
	}
	curVal := parseZonesData(current.Contents)

	changes := make(chan *ZonesForPartitionWatchData, 10)

	go func() {
		defer close(changes)

		for wd := range wdChannel {
			if wd != nil {
				changes <- &ZonesForPartitionWatchData{Err: wd.Err}
				return
			}

			changes <- &ZonesForPartitionWatchData{zones: parseZonesData(wd.Contents)}
		}
	}()

	return &ZonesForPartitionWatchData{zones: curVal}, changes, wdCancel
}

func (s *TopoServer) WatchPartitions(ctx context.Context) (error, []*PartitionTopo, <-chan *PartitionWatchData, CancelFunc) {
	if ctx == nil {
		return ErrNoNode, nil, nil, nil
	}

	dirPath := path.Join(partitionsPath) + "/"
	partitionIds, dirVersion, err := s.backend.ListDir(ctx, GlobalZone, dirPath)
	if err != nil && err != ErrNoNode {
		return err, nil, nil, nil
	}

	var partitionTopos []*PartitionTopo
	if err != ErrNoNode && len(partitionIds) != 0 {
		partitionTopos = make([]*PartitionTopo, 0, len(partitionIds))
		for _, partitionId := range partitionIds {
			contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(partitionsPath, fmt.Sprint(partitionId),
				PartitionTopoFile))
			if err != nil {
				return err, nil, nil, nil
			}

			partitionMeta := &metapb.Partition{}
			if err := proto.Unmarshal(contents, partitionMeta); err != nil {
				log.Error("Fail to unmarshal meta data for partition[%d]. err[%v]", partitionId, err)
				return err, nil, nil, nil
			}

			partition := &PartitionTopo{Version: version, Partition: partitionMeta}
			partitionTopos = append(partitionTopos, partition)
		}
	}

	wdChannel, cancel, err := s.backend.WatchDir(ctx, GlobalZone, dirPath, dirVersion)
	if err != nil {
		return err, nil, nil, nil
	}

	changes := make(chan *PartitionWatchData, 10)

	go func() {
		defer close(changes)

		for wd := range wdChannel {
			if wd != nil {
				changes <- &PartitionWatchData{Err: wd.Err}
				return
			}

			value := &metapb.Partition{}
			if err := proto.Unmarshal(wd.Contents, value); err != nil {
				log.Error("Fail to unmarshal meta data for partition. err[%v]", err)
				cancel()
				for range wdChannel {
				}
				changes <- &PartitionWatchData{Err: err}
				return
			}

			changes <- &PartitionWatchData{PartitionTopo: &PartitionTopo{Partition: value, Version: wd.Version}}
		}
	}()

	return nil, partitionTopos, changes, cancel
}
