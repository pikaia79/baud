package gm

import (
	"context"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sync"
	"sync/atomic"
	"time"
)

//go:generate mockgen -destination zm_rpc_client_mock.go -package gm github.com/tiglabs/baudengine/gm ZoneMasterRpcClient
const (
	ZONE_MASTER_GRPC_REQUEST_TIMEOUT = 5 * time.Second
)

var (
	zmClientSingle     *ZoneMasterRpcClientImpl
	zmClientSingleLock sync.Mutex
	zmClientSingleDone uint32
)

type ZoneMasterRpcClient interface {
	CreatePartition(addr string, partition *metapb.Partition) (*metapb.Replica, error)
	DeletePartition(addr string, partitionId metapb.PartitionID) error
	AddReplica(addr string, partitionId metapb.PartitionID, replica *metapb.Replica) error
	RemoveReplica(addr string, partitionId metapb.PartitionID, replica *metapb.Replica) error
	Close()
}

type ZoneMasterRpcClientImpl struct {
	ctx       context.Context
	cancel    context.CancelFunc
	rpcClient *rpc.Client
}

func GetZoneMasterRpcClientSingle(config *Config) ZoneMasterRpcClient {
	if zmClientSingle != nil {
		return zmClientSingle
	}
	if atomic.LoadUint32(&zmClientSingleDone) == 1 {
		return zmClientSingle
	}

	zmClientSingleLock.Lock()
	defer zmClientSingleLock.Unlock()

	if atomic.LoadUint32(&zmClientSingleDone) == 0 {
		if config == nil {
			log.Error("config should not be nil at first time when create ZoneMasterRpcClient single")
		}

		zmClientSingle = new(ZoneMasterRpcClientImpl)
		zmClientSingle.ctx, zmClientSingle.cancel = context.WithCancel(context.Background())

		connMgrOpt := rpc.DefaultManagerOption
		connMgr := rpc.NewConnectionMgr(zmClientSingle.ctx, &connMgrOpt)
		clientOpt := rpc.DefaultClientOption
		clientOpt.ClusterID = config.ClusterCfg.ClusterID
		clientOpt.ConnectMgr = connMgr
		clientOpt.CreateFunc = func(cc *grpc.ClientConn) interface{} { return masterpb.NewMasterRpcClient(cc) }
		zmClientSingle.rpcClient = rpc.NewClient(1, &clientOpt)

		atomic.StoreUint32(&zmClientSingleDone, 1)

		log.Info("ZoneMasterRpcClient single has started")
	}

	return zmClientSingle
}

func (c *ZoneMasterRpcClientImpl) Close() {
	zmClientSingleLock.Lock()
	defer zmClientSingleLock.Unlock()

	if c.rpcClient != nil {
		c.rpcClient.Close()
		c.rpcClient = nil
	}

	zmClientSingle = nil
	atomic.StoreUint32(&zmClientSingleDone, 0)

	log.Info("ZoneMasterRpcClient single has closed")
}

func (c *ZoneMasterRpcClientImpl) getClient(addr string) (masterpb.MasterRpcClient, error) {
	client, err := c.rpcClient.GetGrpcClient(addr)
	if err != nil {
		log.Error("fail to get grpc client[%v] handle from pool. err[%v]", addr, err)
		return nil, ErrRpcGetClientFailed
	}
	return client.(masterpb.MasterRpcClient), nil
}

func (c *ZoneMasterRpcClientImpl) CreatePartition(addr string, partition *metapb.Partition) (*metapb.Replica, error) {
	log.Info("create partition[%d] into addr[%s]", partition, addr)

	client, err := c.getClient(addr)
	if err != nil {
		return nil, err
	}

	req := &masterpb.CreatePartitionRequest{
		RequestHeader: metapb.RequestHeader{},
		Partition:     *partition,
	}
	ctx, cancel := context.WithTimeout(context.Background(), ZONE_MASTER_GRPC_REQUEST_TIMEOUT)
	defer cancel()
	resp, err := client.CreatePartition(ctx, req)
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return nil, ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == metapb.RESP_CODE_OK {
		return &resp.Replica, nil
	} else {
		log.Error("grpc CreatePartition response err[%v]", resp.ResponseHeader)
		return nil, ErrRpcInvokeFailed
	}
}

func (c *ZoneMasterRpcClientImpl) DeletePartition(addr string, partitionId metapb.PartitionID) error {
	log.Info("delete partitionId[%d] into addr[%s]", partitionId, addr)
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &masterpb.DeletePartitionRequest{
		RequestHeader: metapb.RequestHeader{},
		PartitionID:   partitionId,
	}
	ctx, cancel := context.WithTimeout(context.Background(), ZONE_MASTER_GRPC_REQUEST_TIMEOUT)
	defer cancel()
	resp, err := client.DeletePartition(ctx, req)
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == metapb.RESP_CODE_OK {
		return nil
	} else {
		log.Error("grpc DeletePartition response err[%v]", resp.ResponseHeader)
		return ErrRpcInvokeFailed
	}
}

func (c *ZoneMasterRpcClientImpl) AddReplica(addr string, partitionId metapb.PartitionID, replica *metapb.Replica) error {
	log.Info("add replica[%v] of partitionId[%s] into addr[%s]", replica, partitionId, addr)
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &masterpb.ChangeReplicaRequest{
		RequestHeader: metapb.RequestHeader{},
		Type:          masterpb.ReplicaChangeType_Add,
		PartitionID:   partitionId,
		Replica:       *replica,
	}
	ctx, cancel := context.WithTimeout(context.Background(), ZONE_MASTER_GRPC_REQUEST_TIMEOUT)
	defer cancel()
	resp, err := client.ChangeReplica(ctx, req)
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == metapb.RESP_CODE_OK {
		return nil
	} else {
		log.Error("grpc ChangeReplica(add) response err[%v]", resp.ResponseHeader)
		return ErrRpcInvokeFailed
	}
}

func (c *ZoneMasterRpcClientImpl) RemoveReplica(addr string, partitionId metapb.PartitionID, replica *metapb.Replica) error {
	log.Info("remove replica[%v] of partitionId[%d] into addr[%s]", replica, partitionId, addr)
	client, err := c.getClient(addr)
	if err != nil {
		return err
	}

	req := &masterpb.ChangeReplicaRequest{
		RequestHeader: metapb.RequestHeader{},
		Type:          masterpb.ReplicaChangeType_Remove,
		PartitionID:   partitionId,
		Replica:       *replica,
	}
	ctx, cancel := context.WithTimeout(context.Background(), ZONE_MASTER_GRPC_REQUEST_TIMEOUT)
	defer cancel()
	resp, err := client.ChangeReplica(ctx, req)
	if err != nil {
		if status, ok := status.FromError(err); ok {
			err = status.Err()
		}
		log.Error("grpc invoke is failed. err[%v]", err)
		return ErrRpcInvokeFailed
	}

	if resp.ResponseHeader.Code == metapb.RESP_CODE_OK {
		return nil
	} else {
		log.Error("grpc ChangeReplica(remove) response err[%v]", resp.ResponseHeader)
		return ErrRpcInvokeFailed
	}
}
