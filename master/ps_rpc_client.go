package master

import (
	"context"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sync"
	"time"
	"github.com/tiglabs/baud/util/log"
)

const (
	GRPC_REQUEST_TIMEOUT = time.Second
	GRPC_CONN_TIMEOUT    = time.Second * 3
)

var (
	singleInstance *PSRpcClient
	instanceLock   sync.RWMutex
)

type PSRpcClient struct {
	connPool map[string]*PSConn
	lock     sync.RWMutex
}

func GetPSRpcClientInstance() *PSRpcClient {
	if singleInstance != nil {
		return singleInstance
	}

	instanceLock.Lock()
	defer instanceLock.Unlock()
	if singleInstance == nil {
		singleInstance = &PSRpcClient{
			connPool: make(map[string]*PSConn),
		}
	}
	return singleInstance
}

type PSConn struct {
	rpcAddr string
	conn    *grpc.ClientConn
	client  pspb.AdminGrpcClient
}

func (c *PSConn) callRpc(req interface{}, timeout time.Duration) (resp interface{}, err error) {
	var header *metapb.ResponseHeader

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	switch in := req.(type) {
	case *pspb.CreatePartitionRequest:
		out, err := c.client.CreatePartition(ctx, in)
		cancel()
		if err != nil {
			if status, ok := status.FromError(err); ok {
				err = status.Err()
			}
			log.Error("grpc invoke is failed. err[%v]", err)
			return nil, ErrGrpcInvokeFailed
		}

		header = &out.ResponseHeader
		if header == nil {
			return nil, ErrGrpcInvalidResp
		}
		if header.Code == 0 {
			return out, nil
		}

	case *pspb.DeletePartitionRequest:
		out, err := c.client.DeletePartition(ctx, in)
		cancel()
		if err != nil {
			if status, ok := status.FromError(err); ok {
				err = status.Err()
			}
			log.Error("grpc invoke is failed. err[%v]", err)
			return nil, ErrGrpcInvokeFailed
		}

		header = &out.ResponseHeader
		if header == nil {
			return nil, ErrGrpcInvalidResp
		}
		if header.Code == 0 {
			return out, nil
		}

	case *pspb.ChangeReplicaRequest:
		out, err := c.client.ChangeReplica(ctx, in)
		cancel()
		if err != nil {
			if status, ok := status.FromError(err); ok {
				err = status.Err()
			}
			log.Error("grpc invoke is failed. err[%v]", err)
			return nil, ErrGrpcInvokeFailed
		}

		header = &out.ResponseHeader
		if header == nil {
			return nil, ErrGrpcInvalidResp
		}
		if header.Code == 0 {
			return out, nil
		}

	default:
		cancel()
		log.Error("invalid grpc request type[%v]", in)
		return nil, ErrInternalError
	}

	log.Error("grpc invoke return error message[%v]", header.Message)
	return nil, ErrGrpcInvokeFailed
}

func (c *PSRpcClient) CreatePartition(addr string, partition *metapb.Partition) error {
	psConn, err := c.getConn(addr)
	if err != nil {
		return err
	}

	req := &pspb.CreatePartitionRequest{
		RequestHeader: metapb.RequestHeader{},
		Partition:     *partition,
	}
	_, err = psConn.callRpc(req, GRPC_REQUEST_TIMEOUT)
	if err != nil {
		return err
	}

	return nil
}

func (c *PSRpcClient) DeletePartition(addr string, partitionId metapb.PartitionID) error {
	psConn, err := c.getConn(addr)
	if err != nil {
		return err
	}

	req := &pspb.DeletePartitionRequest{
		RequestHeader: metapb.RequestHeader{},
		ID:            partitionId,
	}
	_, err = psConn.callRpc(req, GRPC_REQUEST_TIMEOUT)
	if err != nil {
		return err
	}

	return nil
}

func (c *PSRpcClient) AddReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
	replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error {
	psConn, err := c.getConn(addr)
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
	_, err = psConn.callRpc(req, GRPC_REQUEST_TIMEOUT)
	if err != nil {
		return err
	}

	return nil
}

func (c *PSRpcClient) RemoveReplica(addr string, partitionId metapb.PartitionID, raftAddrs *metapb.RaftAddrs,
	replicaId metapb.ReplicaID, replicaNodeId metapb.NodeID) error {
	psConn, err := c.getConn(addr)
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
	_, err = psConn.callRpc(req, GRPC_REQUEST_TIMEOUT)
	if err != nil {
		return err
	}

	return nil
}

func (c *PSRpcClient) getConn(addr string) (*PSConn, error) {
	if len(addr) == 0 {
		return nil, ErrInternalError
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	if psConn, ok := c.connPool[addr]; ok {
		return psConn, nil
	}

	ctx, _ := context.WithTimeout(context.Background(), GRPC_CONN_TIMEOUT)
	grpcConn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		log.Error("fail to create grpc raw client connection for addr[%v]. err[%v]", addr, err)
		return nil, err
	}

	psConn := &PSConn{
		rpcAddr: addr,
		conn:    grpcConn,
		client:  pspb.NewAdminGrpcClient(grpcConn),
	}
	c.connPool[addr] = psConn

	return psConn, nil
}
