package gm

import (
	"github.com/golang/mock/gomock"
	"github.com/tiglabs/baudengine/proto/metapb"
	"testing"
)

func TestCreatePartition(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	mockZoneMasterRpcClient := NewMockZoneMasterRpcClient(mockCtl)
	mockZoneMasterRpcClient.EXPECT().CreatePartition(
		"127.0.0.1",
		&metapb.Partition{
			ID:        1,
			DB:        2,
			Space:     3,
			StartSlot: 1,
			EndSlot:   1000,
			Replicas: []metapb.Replica{
				{
					ID:   uint64(4),
					Zone: "beijing",
				},
			},
		}).Return(&metapb.Replica{
		ID:     uint64(4),
		Zone:   "beijing",
		NodeID: 10,
	}, nil)
	mockZoneMasterRpcClient.CreatePartition(
		"127.0.0.1",
		&metapb.Partition{
			ID:        1,
			DB:        2,
			Space:     3,
			StartSlot: 1,
			EndSlot:   1000,
			Replicas: []metapb.Replica{
				{
					ID:   uint64(4),
					Zone: "beijing",
				},
			},
		})
}

func TestDeletePartition(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	mockZoneMasterRpcClient := NewMockZoneMasterRpcClient(mockCtl)
	mockZoneMasterRpcClient.EXPECT().DeletePartition(
		"127.0.0.1",
		uint64(1)).Return(nil)
	mockZoneMasterRpcClient.DeletePartition(
		"127.0.0.1",
		uint64(1))
}

func TestAddReplica(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	mockZoneMasterRpcClient := NewMockZoneMasterRpcClient(mockCtl)
	mockZoneMasterRpcClient.EXPECT().AddReplica(
		"127.0.0.1",
		uint64(1),
		&metapb.Replica{
			ID:   1,
			Zone: "beijing",
		}).Return(nil)
	mockZoneMasterRpcClient.AddReplica(
		"127.0.0.1",
		uint64(1),
		&metapb.Replica{
			ID:   1,
			Zone: "beijing",
		})
}

func TestRemoveReplica(t *testing.T) {
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	mockZoneMasterRpcClient := NewMockZoneMasterRpcClient(mockCtl)
	mockZoneMasterRpcClient.EXPECT().RemoveReplica(
		"127.0.0.1",
		uint64(1),
		&metapb.Replica{
			ID:   1,
			Zone: "beijing",
		}).Return(nil)
	mockZoneMasterRpcClient.RemoveReplica(
		"127.0.0.1",
		uint64(1),
		&metapb.Replica{
			ID:   1,
			Zone: "beijing",
		})
}
