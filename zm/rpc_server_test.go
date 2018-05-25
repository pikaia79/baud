package zm

import (
    "testing"
    "github.com/tiglabs/baudengine/proto/masterpb"
    "github.com/tiglabs/baudengine/proto/metapb"
    "github.com/golang/mock/gomock"
    "github.com/tiglabs/baudengine/util/assert"
    "github.com/tiglabs/baudengine/util/log"
    "time"
    "math/rand"
)

const (
    T_PSID_MAX = 3
    T_PSID_START = 1

    T_DB1           = "db1"
    T_DBID          = 1000
    T_SPACE1        = "space1"

    T_PARTITION_MAX     = 2
    T_PARTITIONID_START = 10

    T_REPLICA_MAX     = 2
    T_REPLICAID_START = 100
)

func TestPSRegister(t *testing.T) {


}

func TestPSHeartbeatNormal(t *testing.T) {
    defer time.Sleep(time.Second)

    ctrl := gomock.NewController(t)
    defer ctrl.Finish()

    assert.GreaterEqual(t, T_PSID_MAX, T_REPLICA_MAX)

    mockStore, _, _ := CreateStoreMocks(ctrl)
    MockPushEvent(ctrl)
    cluster := NewCluster(nil, mockStore)
    rpcServer := new(RpcServer)
    rpcServer.cluster = cluster

    InitPsCache(cluster)
    InitPartitionCache(cluster)

    log.Debug("BEGIN to confVerHb == confVerMS == 0")
    // validate non-leader hb results under confVerHb == confVerMS and confVerMS == 0
    for psIdx := 0; psIdx < T_PSID_MAX; psIdx++ {
        psId := T_PSID_START + psIdx

        // init param
        var leaderPsId = T_PSID_START
        var replicaMax = 1
        var leaderReplicaId = T_REPLICAID_START
        var confVer = 0
        assert.LessEqual(t, replicaMax, T_REPLICA_MAX)
        assert.LessEqual(t, leaderReplicaId, T_REPLICAID_START+replicaMax-1)
        assert.LessEqual(t, leaderPsId, T_PSID_START+replicaMax-1)

        if psId != leaderPsId {
            req := NewPSHeartbeatRequest(t, psId, leaderPsId, replicaMax, leaderReplicaId, confVer, 0)
            rpcServer.PSHeartbeat(nil, req)

            for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
                partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
                partition := cluster.PartitionCache.FindPartitionById(partitionId)
                assert.NotNil(t, partition)

                assert.Nil(t, partition.Leader)
                assert.Nil(t, partition.Replicas)

                assert.Equal(t, partition.Epoch.ConfVersion, uint64(confVer), "epoch confVersion != 0")
                assert.Equal(t, partition.Epoch.Version, uint64(0), "epoch Version != 0")
            }
            break
        }
    }

    // validate leader hb results under confVerHb == confVerMS and confVerMS == 0
    for psIdx := 0; psIdx < T_PSID_MAX; psIdx++ {
        psId := T_PSID_START + psIdx

        // init param
        var leaderPsId = T_PSID_START
        var replicaMax = 1
        var leaderReplicaId = T_REPLICAID_START
        var confVer = 0
        assert.LessEqual(t, replicaMax, T_REPLICA_MAX)
        assert.LessEqual(t, leaderReplicaId, T_REPLICAID_START + replicaMax - 1)
        assert.LessEqual(t, leaderPsId, T_PSID_START + replicaMax - 1)

        if psId == leaderPsId {
            req := NewPSHeartbeatRequest(t, psId, leaderPsId, replicaMax, leaderReplicaId, confVer, 0)
            rpcServer.PSHeartbeat(nil, req)

            for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
                partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
                partition := cluster.PartitionCache.FindPartitionById(partitionId)
                assert.NotNil(t, partition)

                assert.NotNil(t, partition.Leader)
                assert.Equal(t, partition.Leader.ID, metapb.ReplicaID(leaderReplicaId), "unmatched leader replicaid")
                assert.Equal(t, partition.Leader.NodeID, metapb.NodeID(leaderPsId), "unmatched leader nodeid")

                assert.NotNil(t, partition.Replicas)
                assert.Equal(t, len(partition.Replicas), replicaMax, "unmatched number of replicas")

                for rIdx := 0; rIdx < replicaMax; rIdx++ {
                    var replicaId = metapb.ReplicaID(T_REPLICAID_START+rIdx)

                    // order of replica is not necessary from small to large by replicaid
                    var found bool
                    var replica metapb.Replica
                    for _, replica = range partition.Replicas {
                        if replicaId == replica.ID {
                            found = true
                            break
                        }
                    }
                    assert.True(t, found)
                    assert.Equal(t, replica.ID, metapb.ReplicaID(leaderReplicaId), "unmatched leader replicaid")
                    assert.Equal(t, replica.NodeID, metapb.NodeID(leaderPsId), "unmatched leader nodeid")
                }

                assert.Equal(t, partition.Epoch.ConfVersion, uint64(confVer), "epoch confVersion != 0")
                assert.Equal(t, partition.Epoch.Version, uint64(0), "epoch Version != 0")
            }
            break
        }
    }

    // validate leader and non-leader hb results at times, but partition cache not changed
    for times := 0; times < 3; times++ {
        // init param
        var leaderPsId = T_PSID_START
        var replicaMax = 1
        var leaderReplicaId = T_REPLICAID_START
        var confVer = 0
        assert.LessEqual(t, replicaMax, T_REPLICA_MAX)
        assert.LessEqual(t, leaderReplicaId, T_REPLICAID_START+replicaMax-1)
        assert.LessEqual(t, leaderPsId, T_PSID_START+replicaMax-1)

        for psIdx := 0; psIdx < T_PSID_MAX; psIdx++ {
            psId := T_PSID_START + psIdx

            req := NewPSHeartbeatRequest(t, psId, leaderPsId, replicaMax, leaderReplicaId, confVer, 0)
            rpcServer.PSHeartbeat(nil, req)

            for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
                partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
                partition := cluster.PartitionCache.FindPartitionById(partitionId)
                assert.NotNil(t, partition)

                assert.NotNil(t, partition.Leader)
                assert.Equal(t, partition.Leader.ID, metapb.ReplicaID(leaderReplicaId), "unmatched leader replicaid")
                assert.Equal(t, partition.Leader.NodeID, metapb.NodeID(leaderPsId), "unmatched leader nodeid")

                assert.NotNil(t, partition.Replicas)
                assert.Equal(t, len(partition.Replicas), replicaMax, "unmatched number of replicas")

                for rIdx := 0; rIdx < replicaMax; rIdx++ {
                    var replicaId = metapb.ReplicaID(T_REPLICAID_START + rIdx)

                    // order of replica is not necessary from small to large by replicaid
                    var found bool
                    var replica metapb.Replica
                    for _, replica = range partition.Replicas {
                        if replicaId == replica.ID {
                            found = true
                            break
                        }
                    }
                    assert.True(t, found)
                    assert.Equal(t, replica.ID, metapb.ReplicaID(T_REPLICAID_START+rIdx), "unmatched replicaid")
                    assert.Equal(t, replica.NodeID, metapb.NodeID(T_PSID_START+rIdx), "unmatched replica nodeid")
                }

                assert.Equal(t, partition.Epoch.ConfVersion, uint64(confVer), "epoch confVersion != 0")
                assert.Equal(t, partition.Epoch.Version, uint64(0), "epoch Version != 0")
            }
        }
    }

    log.Debug("BEGIN to confVerHb > confVerMS")
    // validate leader hb with confVer greater than 0 under confVerHb > confVerMS
    var lastLeaderPsId, lastReplicaMax, lastLeaderReplicaId, lastConfVer int
    {
        // init param
        var replicaMax = T_REPLICA_MAX
        var leaderPsId = T_PSID_START + replicaMax - 1
        var leaderReplicaId = T_REPLICAID_START + T_REPLICA_MAX - 1
        var confVer = 2
        assert.LessEqual(t, replicaMax, T_REPLICA_MAX)
        assert.LessEqual(t, leaderReplicaId, T_REPLICAID_START + replicaMax - 1)
        assert.LessEqual(t, leaderPsId, T_PSID_START + replicaMax - 1)

        req := NewPSHeartbeatRequest(t, leaderPsId, leaderPsId, replicaMax, leaderReplicaId, confVer, 0)
        rpcServer.PSHeartbeat(nil, req)
        for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
            partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
            partition := cluster.PartitionCache.FindPartitionById(partitionId)
            assert.NotNil(t, partition)

            assert.NotNil(t, partition.Leader)
            assert.Equal(t, partition.Leader.ID, metapb.ReplicaID(leaderReplicaId), "unmatched leader replicaid")
            assert.Equal(t, partition.Leader.NodeID, metapb.NodeID(leaderPsId), "unmatched leader nodeid")

            assert.NotNil(t, partition.Replicas)
            assert.Equal(t, len(partition.Replicas), replicaMax, "unmatched number of replicas")

            for rIdx := 0; rIdx < replicaMax; rIdx++ {
                var replicaId = metapb.ReplicaID(T_REPLICAID_START + rIdx)

                // order of replica is not necessary from small to large by replicaid
                var found bool
                var replica metapb.Replica
                for _, replica = range partition.Replicas {
                    if replicaId == replica.ID {
                        found = true
                        break
                    }
                }
                assert.True(t, found)
                assert.Equal(t, replica.ID, metapb.ReplicaID(T_REPLICAID_START+rIdx), "unmatched replicaid")
                assert.Equal(t, replica.NodeID, metapb.NodeID(T_PSID_START+rIdx), "unmatched replica nodeid")
            }

            assert.Equal(t, partition.Epoch.ConfVersion, uint64(confVer), "epoch confVersion != 2")
            assert.Equal(t, partition.Epoch.Version, uint64(0), "epoch Version != 0")
        }

        lastLeaderPsId = leaderPsId
        lastReplicaMax = replicaMax
        lastLeaderReplicaId = leaderReplicaId
        lastConfVer = confVer
    }

    log.Debug("BEGIN to confVerHb < confVerMS")
    // validate leader hb with confVer under confVerHb < confVerMS
    {
        // init param
        var leaderPsId = T_PSID_START
        var replicaMax = 1
        var leaderReplicaId = T_REPLICAID_START
        var confVer = 1
        assert.LessEqual(t, replicaMax, T_REPLICA_MAX)
        assert.LessEqual(t, leaderReplicaId, T_REPLICAID_START + replicaMax - 1)
        assert.LessEqual(t, leaderPsId, T_PSID_START + replicaMax - 1)

        for psIdx := 0; psIdx < T_PSID_MAX; psIdx++ {
            psId := T_PSID_START + psIdx

            req := NewPSHeartbeatRequest(t, psId, leaderPsId, replicaMax, leaderReplicaId, confVer, 0)
            rpcServer.PSHeartbeat(nil, req)

            for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
                partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
                partition := cluster.PartitionCache.FindPartitionById(partitionId)
                assert.NotNil(t, partition)

                assert.NotNil(t, partition.Leader)
                assert.Equal(t, partition.Leader.ID, metapb.ReplicaID(lastLeaderReplicaId), "unmatched leader replicaid")
                assert.Equal(t, partition.Leader.NodeID, metapb.NodeID(lastLeaderPsId), "unmatched leader nodeid")

                assert.NotNil(t, partition.Replicas)
                assert.Equal(t, len(partition.Replicas), lastReplicaMax, "unmatched number of replicas")

                for rIdx := 0; rIdx < len(partition.Replicas); rIdx++ {
                    var replicaId = metapb.ReplicaID(T_REPLICAID_START + rIdx)

                    // order of replica is not necessary from small to large by replicaid
                    var found bool
                    var replica metapb.Replica
                    for _, replica = range partition.Replicas {
                        if replicaId == replica.ID {
                            found = true
                            break
                        }
                    }
                    assert.True(t, found)
                    assert.Equal(t, replica.ID, metapb.ReplicaID(T_REPLICAID_START+rIdx), "unmatched replicaid")
                    assert.Equal(t, replica.NodeID, metapb.NodeID(T_PSID_START+rIdx), "unmatched replica nodeid")
                }

                assert.Equal(t, partition.Epoch.ConfVersion, uint64(lastConfVer), "unmatched confVersion")
                assert.Equal(t, partition.Epoch.Version, uint64(0), "unmatched Version")
            }
        }
    }

    log.Debug("BEGIN to confVerHb == confVerMS != 0 and valid leader changed")
    // validate leader hb with confVer greater then zero under confVerHb == confVerMS, and valid leader changed
    {
        // init param
        assert.Greater(t, lastReplicaMax, 1)
        var replicaMax= lastReplicaMax
        var confVer= lastConfVer
        var leaderReplicaId = lastLeaderReplicaId
        if leaderReplicaId > T_REPLICAID_START + replicaMax - 1 {
            leaderReplicaId = T_REPLICAID_START
        }
        var leaderPsId= T_PSID_START + (leaderReplicaId - T_REPLICAID_START)
        assert.LessEqual(t, replicaMax, T_REPLICA_MAX)
        assert.LessEqual(t, leaderReplicaId, T_REPLICAID_START + replicaMax - 1)
        assert.LessEqual(t, leaderPsId, T_PSID_START + replicaMax - 1)


        req := NewPSHeartbeatRequest(t, leaderPsId, leaderPsId, replicaMax, leaderReplicaId, confVer, 0)
        rpcServer.PSHeartbeat(nil, req)
        for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
            partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
            partition := cluster.PartitionCache.FindPartitionById(partitionId)
            assert.NotNil(t, partition)

            assert.NotNil(t, partition.Leader)
            assert.Equal(t, partition.Leader.ID, metapb.ReplicaID(leaderReplicaId), "unmatched leader replicaid")
            assert.Equal(t, partition.Leader.NodeID, metapb.NodeID(leaderPsId), "unmatched leader nodeid")

            assert.NotNil(t, partition.Replicas)
            assert.Equal(t, len(partition.Replicas), replicaMax, "unmatched number of replicas")

            for rIdx := 0; rIdx < replicaMax; rIdx++ {
                var replicaId = metapb.ReplicaID(T_REPLICAID_START + rIdx)

                // order of replica is not necessary from small to large by replicaid
                var found bool
                var replica metapb.Replica
                for _, replica = range partition.Replicas {
                    if replicaId == replica.ID {
                        found = true
                        break
                    }
                }
                assert.True(t, found)
                assert.Equal(t, replica.ID, metapb.ReplicaID(T_REPLICAID_START+rIdx), "unmatched replicaid")
                assert.Equal(t, replica.NodeID, metapb.NodeID(T_PSID_START+rIdx), "unmatched replica nodeid")
            }

            assert.Equal(t, partition.Epoch.ConfVersion, uint64(confVer), "epoch confVersion != 2")
            assert.Equal(t, partition.Epoch.Version, uint64(0), "epoch Version != 0")
        }

        lastLeaderPsId = leaderPsId
        lastReplicaMax = replicaMax
        lastLeaderReplicaId = leaderReplicaId
        lastConfVer = confVer
    }

    log.Debug("BEGIN to confVerHb == confVerMS != 0 and invalid leader")
    // validate leader hb with confVer greater then zero under confVerHb == confVerMS, and invalid leader
    {
        // init param
        assert.Greater(t, lastReplicaMax, 1)
        var replicaMax= lastReplicaMax
        var confVer= lastConfVer
        var leaderReplicaId = T_REPLICAID_START + 999999
        var leaderPsId = lastLeaderPsId
        assert.LessEqual(t, replicaMax, T_REPLICA_MAX)
        assert.LessEqual(t, leaderPsId, T_PSID_START+replicaMax-1)

        req := NewPSHeartbeatRequest(t, leaderPsId, leaderPsId, replicaMax, leaderReplicaId, confVer, 0)
        rpcServer.PSHeartbeat(nil, req)

        for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
            partitionId := metapb.PartitionID(T_PARTITIONID_START + pIdx)
            partition := cluster.PartitionCache.FindPartitionById(partitionId)
            assert.NotNil(t, partition)

            assert.NotNil(t, partition.Leader)
            assert.Equal(t, partition.Leader.ID, metapb.ReplicaID(lastLeaderReplicaId), "unmatched leader replicaid")
            assert.Equal(t, partition.Leader.NodeID, metapb.NodeID(lastLeaderPsId), "unmatched leader nodeid")

            assert.NotNil(t, partition.Replicas)
            assert.Equal(t, len(partition.Replicas), lastReplicaMax, "unmatched number of replicas")

            for rIdx := 0; rIdx < len(partition.Replicas); rIdx++ {
                var replicaId = metapb.ReplicaID(T_REPLICAID_START + rIdx)

                // order of replica is not necessary from small to large by replicaid
                var found bool
                var replica metapb.Replica
                for _, replica = range partition.Replicas {
                    if replicaId == replica.ID {
                        found = true
                        break
                    }
                }
                assert.True(t, found)
                assert.Equal(t, replica.ID, metapb.ReplicaID(T_REPLICAID_START+rIdx), "unmatched replicaid")
                assert.Equal(t, replica.NodeID, metapb.NodeID(T_PSID_START+rIdx), "unmatched replica nodeid")
            }

            assert.Equal(t, partition.Epoch.ConfVersion, uint64(lastConfVer), "unmatched confVersion")
            assert.Equal(t, partition.Epoch.Version, uint64(0), "unmatched Version")
        }
    }
}

func TestPSHeartbeatPartitionNotFound(t *testing.T) {
   defer time.Sleep(time.Second)

   ctrl := gomock.NewController(t)
   defer ctrl.Finish()

   assert.GreaterEqual(t, T_PSID_MAX, T_REPLICA_MAX)

   mockStore, _, _ := CreateStoreMocks(ctrl)
   MockPushEvent(ctrl)
   cluster := NewCluster(nil, mockStore)
   rpcServer := new(RpcServer)
   rpcServer.cluster = cluster

   InitPsCache(cluster)
   InitPartitionCache(cluster)

   // validate PartitionId not found
   req := new(masterpb.PSHeartbeatRequest)
   req.NodeID = metapb.NodeID(T_PSID_START)
   req.Partitions = make([]masterpb.PartitionInfo, 0, T_PARTITION_MAX)
   for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
       info := new(masterpb.PartitionInfo)
       info.ID = metapb.PartitionID(T_PARTITIONID_START + pIdx +  99999999)

       req.Partitions = append(req.Partitions, *info)
   }

   rpcServer.PSHeartbeat(nil, req)

   for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
       partition := cluster.PartitionCache.FindPartitionById(metapb.PartitionID(T_PARTITIONID_START + pIdx + 99999999))
       assert.Nil(t, partition)
   }
}

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

func MockPushEvent(ctrl *gomock.Controller) {
    processorManagerSingle = &ProcessorManager{
        isStarted: false,
    }
}

func InitPsCache(cluster *Cluster) {
    for psIdx := 0; psIdx < T_PSID_MAX; psIdx++ {
        cluster.PsCache.AddServer(&PartitionServer{
            Node: &metapb.Node{
                ID: metapb.NodeID(T_PSID_START + psIdx),
            },
        })
    }
}

func InitPartitionCache(cluster *Cluster) {
    for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
        cluster.PartitionCache.AddPartition(&Partition{
            Partition: &metapb.Partition{
                ID: metapb.PartitionID(T_PARTITIONID_START + pIdx),
            },
        })
    }
}

func InitPartitionCacheWithReplicas(cluster *Cluster) {
    for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {

        replicas := make([]metapb.Replica, 0, T_REPLICA_MAX)
        for rIdx := 0; rIdx < T_REPLICA_MAX; rIdx++ {
            replica := &metapb.Replica{
                ID: metapb.ReplicaID(T_REPLICAID_START + rIdx),
                NodeID: metapb.NodeID(T_PSID_START + rIdx),
            }
            replicas = append(replicas, *replica)
        }

        cluster.PartitionCache.AddPartition(&Partition{
            Partition: &metapb.Partition{
                ID: metapb.PartitionID(T_PARTITIONID_START + pIdx),
                Replicas: replicas,
            },
        })
    }
}

func NewPSHeartbeatRequest(t *testing.T, psId, leaderPsId, replicaMax, leaderReplicaId,
            confVer, ver int) *masterpb.PSHeartbeatRequest {
    req := new(masterpb.PSHeartbeatRequest)

    req.NodeID = metapb.NodeID(psId)

    req.Partitions = make([]masterpb.PartitionInfo, 0, T_PARTITION_MAX)
    for pIdx := 0; pIdx < T_PARTITION_MAX; pIdx++ {
        info := new(masterpb.PartitionInfo)
        info.ID = metapb.PartitionID(T_PARTITIONID_START + pIdx)

        if psId == leaderPsId {
            info.IsLeader = true
            info.RaftStatus = new(masterpb.RaftStatus)

            // Leader replica
            info.RaftStatus.Replica = metapb.Replica{ID:metapb.ReplicaID(leaderReplicaId),
                    NodeID:metapb.NodeID(leaderPsId)}

            info.RaftStatus.Followers = make([]masterpb.RaftFollowerStatus, 0, T_REPLICA_MAX)
            for rIdx := 0; rIdx < replicaMax; rIdx++ {
                var replicaId = metapb.ReplicaID(T_REPLICAID_START + rIdx)
                if replicaId == metapb.ReplicaID(leaderReplicaId) {
                    continue
                }

                follower := new(masterpb.RaftFollowerStatus)
                follower.ID = replicaId
                follower.NodeID = metapb.NodeID(T_PSID_START + rIdx)

                info.RaftStatus.Followers = append(info.RaftStatus.Followers, *follower)
            }

            info.Epoch.ConfVersion = uint64(confVer)
            info.Epoch.Version = uint64(ver)

        } else {

            info.IsLeader = false

            rand.Seed(time.Now().Unix())
            if rand.Intn(2) == 0 {
                info.RaftStatus = new(masterpb.RaftStatus)
                info.RaftStatus.Replica = metapb.Replica{ID: metapb.ReplicaID(T_REPLICAID_START + 99999999999),
                    NodeID: metapb.NodeID(T_PSID_START + 99999)}
                info.RaftStatus.Followers = make([]masterpb.RaftFollowerStatus, 0, 1)
                follower := new(masterpb.RaftFollowerStatus)
                follower.ID = metapb.ReplicaID(T_REPLICAID_START + 99999999999)
                follower.NodeID = metapb.NodeID(T_PSID_START + 999999999)
                info.RaftStatus.Followers = append(info.RaftStatus.Followers, *follower)
            } else {
                info.RaftStatus = nil
            }

            info.Epoch.ConfVersion = uint64(confVer) + 999999
            info.Epoch.Version = uint64(ver) + 999999
        }

        req.Partitions = append(req.Partitions, *info)
    }

    return req
}


