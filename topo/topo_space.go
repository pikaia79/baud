package topo

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"path"
)

type SpaceTopo struct {
	Version Version
	*metapb.Space
}

type SpaceWatchData struct {
	*SpaceTopo
	Err error
}

func (s *TopoServer) GetAllSpaces(ctx context.Context) ([]*SpaceTopo, error) {
	if ctx == nil {
		return nil, ErrNoNode
	}

	dbAndSpaceIds, _, err := s.backend.ListDir(ctx, GlobalZone, path.Join(spacesPath))
	if err != nil {
		return nil, err
	}
	if dbAndSpaceIds == nil || len(dbAndSpaceIds) == 0 {
		return nil, nil
	}

	spaceTopos := make([]*SpaceTopo, 0)
	for _, dbAndSpaceId := range dbAndSpaceIds {
		contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(spacesPath, dbAndSpaceId, SpaceTopoFile))
		if err != nil {
			return nil, err
		}

		spaceMeta := &metapb.Space{}
		if err := proto.Unmarshal(contents, spaceMeta); err != nil {
			log.Error("Fail to unmarshal meta data for db-space[%d]", dbAndSpaceId)
			return nil, err
		}

		spaceTopo := &SpaceTopo{Version: version, Space: spaceMeta}
		spaceTopos = append(spaceTopos, spaceTopo)
	}

	return spaceTopos, nil
}

func (s *TopoServer) GetSpace(ctx context.Context, dbId metapb.DBID, spaceId metapb.SpaceID) (*SpaceTopo, error) {
	if ctx == nil {
		return nil, ErrNoNode
	}

	nodePath := path.Join(spacesPath, fmt.Sprintf("%d-%d", dbId, spaceId), SpaceTopoFile)
	contents, version, err := s.backend.Get(ctx, GlobalZone, nodePath)
	if err != nil {
		return nil, err
	}

	spaceMeta := &metapb.Space{}
	if err := proto.Unmarshal(contents, spaceMeta); err != nil {
		log.Error("Fail to unmarshal meta data for space[%d]", spaceId)
		return nil, err
	}

	spaceTopo := &SpaceTopo{Version: version, Space: spaceMeta}

	return spaceTopo, nil
}

func (s *TopoServer) AddSpace(ctx context.Context, space *metapb.Space,
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
	txn.Create(path.Join(spacesPath, fmt.Sprintf("%d-%d", space.DB, space.ID), SpaceTopoFile), contents)

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

	spaceTopo := &SpaceTopo{Version: opResults[0].(*TxnCreateOpResult).Version, Space: space}
	partitionTopos := make([]*PartitionTopo, 0)
	for i := 0; i < len(partitions); i++ {
		partitionTopo := &PartitionTopo{Version: opResults[i+1].(*TxnCreateOpResult).Version, Partition: partitions[i]}
		partitionTopos = append(partitionTopos, partitionTopo)
	}

	return spaceTopo, partitionTopos, nil
}

func (s *TopoServer) UpdateSpace(ctx context.Context, space *SpaceTopo) error {
	if ctx == nil || space == nil {
		return ErrNoNode
	}

	nodePath := path.Join(spacesPath, fmt.Sprintf("%d-%d", space.DB, space.ID), SpaceTopoFile)

	contents, err := proto.Marshal(space.Space)
	if err != nil {
		return err
	}

	newVersion, err := s.backend.Update(ctx, GlobalZone, nodePath, contents, space.Version)
	if err != nil {
		return ErrNoNode
	}

	space.Version = newVersion
	return nil
}

func (s *TopoServer) DeleteSpace(ctx context.Context, space *SpaceTopo) error {
	if ctx == nil || space == nil {
		return ErrNoNode
	}

	nodePath := path.Join(spacesPath, fmt.Sprintf("%d-%d", space.DB, space.ID), SpaceTopoFile)
	return s.backend.Delete(ctx, GlobalZone, nodePath, space.Version)
}

func (s *TopoServer) WatchSpaces(ctx context.Context) (error, []*SpaceTopo, <-chan *SpaceWatchData, CancelFunc) {
	if ctx == nil {
		return ErrNoNode, nil, nil, nil
	}

	dirPath := path.Join(spacesPath) + "/"
	dbAndSpaceIds, dirVersion, err := s.backend.ListDir(ctx, GlobalZone, dirPath)
	if err != nil && err != ErrNoNode {
		return err, nil, nil, nil
	}

	var spaceTopos []*SpaceTopo
	if err != ErrNoNode && len(dbAndSpaceIds) != 0 {
		spaceTopos = make([]*SpaceTopo, 0)
		for _, dbAndSpaceId := range dbAndSpaceIds {
			contents, version, err := s.backend.Get(ctx, GlobalZone, path.Join(spacesPath, dbAndSpaceId, SpaceTopoFile))
			if err != nil {
				return err, nil, nil, nil
			}

			spaceMeta := &metapb.Space{}
			if err := proto.Unmarshal(contents, spaceMeta); err != nil {
				log.Error("Fail to unmarshal meta data for db-space[%d]. err[%v]", dbAndSpaceId, err)
				return err, nil, nil, nil
			}

			spaceTopo := &SpaceTopo{Version: version, Space: spaceMeta}
			spaceTopos = append(spaceTopos, spaceTopo)
		}
	}

	wdChannel, cancel, err := s.backend.WatchDir(ctx, GlobalZone, dirPath, dirVersion)
	if err != nil {
		return err, nil, nil, nil
	}

	changes := make(chan *SpaceWatchData, 10)

	go func() {
		defer close(changes)

		for wd := range wdChannel {
			if wd != nil {
				changes <- &SpaceWatchData{Err: wd.Err}
				return
			}

			value := &metapb.Space{}
			if err := proto.Unmarshal(wd.Contents, value); err != nil {
				log.Error("Fail to unmarshal meta data for space. err[%v]", err)
				cancel()
				for range wdChannel {
				}
				changes <- &SpaceWatchData{Err: err}
				return
			}

			changes <- &SpaceWatchData{SpaceTopo: &SpaceTopo{Space: value, Version: wd.Version}}
		}
	}()

	return nil, spaceTopos, changes, cancel
}
