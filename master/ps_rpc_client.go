package master

import (
	"context"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sync"
	"time"
	"sync/atomic"
)

//go:generate mockgen -destination mock/ps_rpc_client_mock.go -package mock_master github.com/tiglabs/baudengine/master PSRpcClient
const (
	PS_GRPC_REQUEST_TIMEOUT = time.Second
)

var (
	psClientSingle     *PSRpcClientImpl
	psClientSingleLock sync.Mutex
	psClientSingleDone uint32
)

type PSRpcClient interface {
    CreatePartition(addr string, partition *metapb.Partition) error
    DeletePartition(addr string, partitionId metapb.PartitionID) error
    AddReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
            replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error
    RemoveReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
            replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error
    Close()
}

type PSRpcClientImpl struct {
	ctx       context.Context
	cancel    context.CancelFunc
	rpcClient *rpc.Client
}

func GetPSRpcClientSingle(config *Config) PSRpcClient {
	if psClientSingle != nil {
		return psClientSingle
	}
	if atomic.LoadUint32(&psClientSingleDone) == 1 {
		return psClientSingle
	}

	psClientSingleLock.Lock()
	defer psClientSingleLock.Unlock()

	if atomic.LoadUint32(&psClientSingleDone) == 0 {
		if config == nil {
			log.Error("config should not be nil at first time when create PSRpcClient single")
		}

		psClientSingle = new(PSRpcClientImpl)
		psClientSingle.ctx, psClientSingle.cancel = context.WithCancel(context.Background())

		connMgrOpt := rpc.DefaultManagerOption
		connMgr := rpc.NewConnectionMgr(psClientSingle.ctx, &connMgrOpt)
		clientOpt := rpc.DefaultClientOption
		clientOpt.ClusterID = config.ClusterCfg.ClusterID
		clientOpt.ConnectMgr = connMgr
		clientOpt.CreateFunc = func(cc *grpc.ClientConn) interface{} { return pspb.NewAdminGrpcClient(cc) }
		psClientSingle.rpcClient = rpc.NewClient(1, &clientOpt)

		atomic.StoreUint32(&psClientSingleDone, 1)

		log.Info("PSRpcClient single has started")
	}

	return psClientSingle
}

func (c *PSRpcClientImpl) Close() {
	psClientSingleLock.Lock()
	defer psClientSingleLock.Unlock()

	if c.rpcClient != nil {
		c.rpcClient.Close()
		c.rpcClient = nil
	}

	psClientSingle = nil
	atomic.StoreUint32(&psClientSingleDone, 0)

	log.Info("PSRpcClient single has closed")
}

func (c *PSRpcClientImpl) getClient(addr string) (pspb.AdminGrpcClient, error) {
	client, err := c.rpcClient.GetGrpcClient(addr)
	if err != nil {
		log.Error("fail to get grpc client[%v] handle from pool. err[%v]", addr, err)
		return nil, ErrRpcGetClientFailed
	}
	return client.(pspb.AdminGrpcClient), nil
}

func (c *PSRpcClientImpl) CreatePartition(addr string, partition *metapb.Partition) error {
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &pspb.CreatePartitionRequest{
		RequestHeader: metapb.RequestHeader{},
		Partition:     *partition,
	}
	ctx, cancel := context.WithTimeout(context.Background(), PS_GRPC_REQUEST_TIMEOUT)
	resp, err := client.CreatePartition(ctx, req)
	cancel()
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == 0 {
		return nil
	} else {
		log.Error("grpc CreatePartition response err[%v]", resp.ResponseHeader)
		return ErrRpcInvokeFailed
	}
}

func (c *PSRpcClientImpl) DeletePartition(addr string, partitionId metapb.PartitionID) error {
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &pspb.DeletePartitionRequest{
		RequestHeader: metapb.RequestHeader{},
		ID:            partitionId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), PS_GRPC_REQUEST_TIMEOUT)
	resp, err := client.DeletePartition(ctx, req)
	cancel()
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == 0 {
		return nil
	} else {
		log.Error("grpc DeletePartition response err[%v]", resp.ResponseHeader)
		return ErrRpcInvokeFailed
	}
}

func (c *PSRpcClientImpl) AddReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
	replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error {
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &pspb.ChangeReplicaRequest{
		RequestHeader: metapb.RequestHeader{},
		Type:          pspb.ReplicaChangeType_Add,
		PartitionID:   partitionId,
		Replica: metapb.Replica{
			ID:        replicaId,
			NodeID:    replicaNodeId,
			RaftAddrs: *raftAddrs,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), PS_GRPC_REQUEST_TIMEOUT)
	resp, err := client.ChangeReplica(ctx, req)
	cancel()
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == 0 {
		return nil
	} else {
		log.Error("grpc ChangeReplica(add) response err[%v]", resp.ResponseHeader)
		return ErrRpcInvokeFailed
	}
}

func (c *PSRpcClientImpl) RemoveReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
	replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error {
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &pspb.ChangeReplicaRequest{
		RequestHeader: metapb.RequestHeader{},
		Type:          pspb.ReplicaChangeType_Remove,
		PartitionID:   partitionId,
		Replica: metapb.Replica{
			ID:        replicaId,
			NodeID:    replicaNodeId,
			RaftAddrs: *raftAddrs,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), PS_GRPC_REQUEST_TIMEOUT)
	resp, err := client.ChangeReplica(ctx, req)
	cancel()
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == 0 {
		return nil
	} else {
		log.Error("grpc ChangeReplica(remove) response err[%v]", resp.ResponseHeader)
		return ErrRpcInvokeFailed
	}
}
