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
)

const (
	PS_GRPC_REQUEST_TIMEOUT = time.Second
)

var (
	single     *PSRpcClient
	singleLock sync.Once
)

type PSRpcClient struct {
	ctx       context.Context
	cancel    context.CancelFunc
	rpcClient *rpc.Client
}

func GetPSRpcClientSingle(config *Config) *PSRpcClient {
	if single == nil {
		singleLock.Do(func() {
			if config == nil {
				log.Panic("config should not be nil at first time when create single psRpcClient")
			}

			single = new(PSRpcClient)
			single.ctx, single.cancel = context.WithCancel(context.Background())

			connMgrOpt := rpc.DefaultManagerOption
			connMgr := rpc.NewConnectionMgr(single.ctx, &connMgrOpt)
			clientOpt := rpc.DefaultClientOption
			clientOpt.ClusterID = string(config.ClusterCfg.ClusterID)
			clientOpt.ConnectMgr = connMgr
			clientOpt.CreateFunc = func(cc *grpc.ClientConn) interface{} { return pspb.NewAdminGrpcClient(cc) }
			single.rpcClient = rpc.NewClient(1, &clientOpt)
		})
	}

	return single
}

func (c *PSRpcClient) Close() {
	c.rpcClient.Close()
}

func (c *PSRpcClient) getClient(addr string) (pspb.AdminGrpcClient, error) {
	client, err := c.rpcClient.GetGrpcClient(addr)
	if err != nil {
		log.Error("fail to get grpc client handle from pool. err[%v]", err)
		return nil, ErrRpcGetClientFailed
	}
	return client.(pspb.AdminGrpcClient), nil
}

func (c *PSRpcClient) CreatePartition(addr string, partition *metapb.Partition) error {
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

func (c *PSRpcClient) DeletePartition(addr string, partitionId metapb.PartitionID) error {
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

func (c *PSRpcClient) AddReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
	replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error {
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &pspb.ChangeReplicaRequest{
		RequestHeader: metapb.RequestHeader{},
		Type:          pspb.ReplicaChangeType_Add,
		PartitionID:   partitionId,
		ReplicaID:     replicaId,
		NodeID:        replicaNodeId,
		RaftAddrs:     *raftAddrs,
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

func (c *PSRpcClient) RemoveReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
	replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error {
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &pspb.ChangeReplicaRequest{
		RequestHeader: metapb.RequestHeader{},
		Type:          pspb.ReplicaChangeType_Remove,
		PartitionID:   partitionId,
		ReplicaID:     replicaId,
		NodeID:        replicaNodeId,
		RaftAddrs:     *raftAddrs,
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
