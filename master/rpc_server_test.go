package master

import (
    "testing"
    "github.com/tiglabs/baudengine/proto/masterpb"
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/golang/mock/gomock"
    "github.com/tiglabs/baudengine/util/assert"
)

const (
    T_PSID_MAX = 3
    T_PSID_START = 1
    T_PSIP = "192.168.0.1"

    T_DB1           = "db1"
    T_DBID          = 1000
    T_SPACE1        = "space1"

    T_PARTITION_MAX     = 2
    T_PARTITIONID_START = 10

    T_REPLICA_MAX     = 3
    T_REPLICAID_START = 100
)

func TestPSRegister(t *testing.T) {


}

func TestPSHeartbeatFirstAdd(t *testing.T) {
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    assert.GreaterEqual(t, T_PSID_MAX, T_REPLICA_MAX)

    mockStore, _, _ := CreateStoreMocks(ctrl)
    cluster := NewCluster(nil, mockStore)
    for psIdx := 0; psIdx < T_PSID_MAX; psIdx++ {
        cluster.PsCache.AddServer(&PartitionServer{
            Node: &metapb.Node{
                ID: metapb.NodeID(T_PSID_START + psIdx),
            },
        })
    }
    for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
        cluster.PartitionCache.AddPartition(&Partition{
            Partition: &metapb.Partition{
                ID: metapb.PartitionID(T_PARTITIONID_START + pIdx),
            },
        })
    }

    rpcServer := new(RpcServer)
    rpcServer.cluster = cluster

    // first hb
    nodeId := metapb.NodeID(T_PSID_START)
    req := NewPSHeartbeatRequest(nodeId)
    rpcServer.PSHeartbeat(nil, req)

    for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
        partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
        partition := cluster.PartitionCache.FindPartitionById(partitionId)
        assert.NotNil(t, partition)

        if pIdx % 2 == 0 {
            assert.NotNil(t, partition.leader)
            assert.NotEqual(t, partition.leader.ID, T_REPLICAID_START + T_REPLICA_MAX - 1, "leader replicaid error")

            assert.NotNil(t, partition.Replicas)
            assert.Equal(t, len(partition.Replicas), T_REPLICA_MAX, "number of replicas error")

            for rIdx := 0; rIdx < T_REPLICA_MAX; rIdx++ {
                replica := partition.Replicas[rIdx]

                assert.NotEqual(t, replica.ID, T_REPLICAID_START+rIdx, "unmatched replicaid")
                assert.NotEqual(t, replica.NodeID, T_PSID_START + rIdx, "unmatched replica nodeid")
            }
        } else {
            assert.Nil(t, partition.leader)
            assert.Nil(t, partition.Replicas)
        }
    }
}
//
//type EmptyStore struct {
//}
//func (m *EmptyStore) Open() error {
//    return nil
//}
//func (m *EmptyStore) Put(key, value []byte) error {
//    return nil
//}
//func (m *EmptyStore) Delete(key []byte) error{
//    return nil
//}
//func (m *EmptyStore) Get(key []byte) ([]byte, error) {
//    return nil, nil
//}
//func (m *EmptyStore) Scan(startKey, limitKey []byte) raftkvstore.Iterator {
//    return nil
//}
//func (m *EmptyStore) NewBatch() Batch {
//    return nil
//}
//func (m *EmptyStore) GetLeaderAsync(leaderChangingCh chan *LeaderInfo) {
//    return
//}
//func (m *EmptyStore) GetLeaderSync() *LeaderInfo {
//    return &LeaderInfo{
//        becomeLeader: true,
//    }
//}
//func (m *EmptyStore) Close() error {
//    return nil
//}

func CreateStoreMocks(ctrl *gomock.Controller) (*MockStore, *MockBatch, *MockIterator) {

    mockIterator := NewMockIterator(ctrl)
    mockIterator.EXPECT().Next().Return(false).AnyTimes()
    mockIterator.EXPECT().Key().Return(nil).AnyTimes()
    mockIterator.EXPECT().Error().Return(nil).AnyTimes()
    mockIterator.EXPECT().Value().Return(nil).AnyTimes()
    mockIterator.EXPECT().Release().AnyTimes()

    mockBatch := NewMockBatch(ctrl)
    mockBatch.EXPECT().Put(gomock.Any(), gomock.Any()).AnyTimes()
    mockBatch.EXPECT().Delete(gomock.Any()).AnyTimes()
    mockBatch.EXPECT().Commit().Return(nil).AnyTimes()

    mockStore := NewMockStore(ctrl)
    mockStore.EXPECT().Open().Return(nil).AnyTimes()
    mockStore.EXPECT().Close().Return(nil).AnyTimes()
    mockStore.EXPECT().Put(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
    mockStore.EXPECT().Delete(gomock.Any()).Return(nil).AnyTimes()
    mockStore.EXPECT().Get(gomock.Any()).Return(nil, nil).AnyTimes()
    mockStore.EXPECT().Scan(gomock.Any(), gomock.Any()).Return(mockIterator).AnyTimes()
    mockStore.EXPECT().NewBatch().Return(mockBatch).AnyTimes()
    mockStore.EXPECT().GetLeaderAsync(gomock.Any()).DoAndReturn(func() {
    }).AnyTimes()
    mockStore.EXPECT().GetLeaderSync().Return(&LeaderInfo{
        becomeLeader: true,
    }).AnyTimes()

    return mockStore, mockBatch, mockIterator
}

func NewPSHeartbeatRequest(nodeId metapb.NodeID) *masterpb.PSHeartbeatRequest {
    req := new(masterpb.PSHeartbeatRequest)

    req.NodeID = nodeId

    req.Partitions = make([]masterpb.PartitionInfo, 0, T_PARTITION_MAX)
    for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
        info := new(masterpb.PartitionInfo)
        info.ID = metapb.PartitionID(T_PARTITIONID_START + pIdx)

        if pIdx % 2 == 0 {
            info.IsLeader = true
            info.RaftStatus = new(masterpb.RaftStatus)
            info.RaftStatus.ID = metapb.ReplicaID(T_REPLICAID_START + T_REPLICA_MAX - 1)

            info.RaftStatus.Followers = make([]masterpb.RaftFollowerStatus, 0, T_REPLICA_MAX)
            for rIdx := 0; rIdx < T_REPLICA_MAX; rIdx++ {
                follower := new(masterpb.RaftFollowerStatus)
                follower.ID = metapb.ReplicaID(T_REPLICAID_START + rIdx)
                follower.NodeID = metapb.NodeID(T_PSID_START + rIdx)

                info.RaftStatus.Followers = append(info.RaftStatus.Followers, *follower)
            }

        } else {

            info.IsLeader = false
            info.RaftStatus = new(masterpb.RaftStatus)
            info.RaftStatus.Followers = make([]masterpb.RaftFollowerStatus, 0, 1)
            follower := new(masterpb.RaftFollowerStatus)
            follower.ID = metapb.ReplicaID(T_REPLICAID_START + 1000)
            follower.NodeID = nodeId
            info.RaftStatus.Followers = append(info.RaftStatus.Followers, *follower)
        }

        req.Partitions = append(req.Partitions, *info)
    }

    return req
}
