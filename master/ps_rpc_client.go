package master

import (
	"sync"
	"google.golang.org/grpc"
	"proto/metapb"
	"proto/partitionserverpb"
	"util/log"
	"time"
	"context"
	"google.golang.org/grpc/status"
)

const (
	GRPC_REQUEST_TIMEOUT = time.Second
	GRPC_CONN_TIMEOUT    = time.Second * 3
)

type PSRpcClient struct {
	connPool map[string]*PSConn
	lock     sync.RWMutex
}

func NewPSRpcClient() *PSRpcClient {
	return &PSRpcClient{
		connPool: make(map[string]*PSConn),
	}
}

type PSConn struct {
	rpcAddr string
	conn    *grpc.ClientConn
	client  pspb.PartitionServerRpcClient
}

func (c *PSConn) callRpc(req interface{}, timeout time.Duration) (resp interface{}, err error) {
	var header *metapb.ResponseHeader

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	switch in := req.(type) {
	case *pspb.CreateReplicaRequest:
		out, err := c.client.CreateReplica(ctx, in)
		cancel()
		if err != nil {
			if status, ok := status.FromError(err); ok {
				err = status.Err()
			}
			log.Error("grpc invoke is failed. err[%v]", err)
			return nil, ErrGrpcInvokeFailed
		}

		header = out.GetHeader()
		if header == nil {
			return nil, ErrGrpcInvalidResp
		}
		if header.Code == 0 {
			return out, nil
		}
	default:
		cancel()
		log.Error("invalid grpc request type[%v]", in)
		return nil, ErrGrpcInvalidReq
	}

	log.Error("grpc invoke return error message[%v]", header.Msg)
	return nil, ErrGrpcInvokeFailed
}

func (c *PSRpcClient) CreateReplica(addr string, partition *metapb.Partition, replicaId uint32, servers []*PartitionServer) error {
	psConn, err := c.getConn(addr)
	if err != nil {
		return err
	}

	req := &pspb.CreateReplicaRequest{
		Header:	new(metapb.RequestHeader),
		Partition: partition,
		ReplicaId: replicaId,
		PartitionServers: servers,
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
		client:  pspb.NewPartitionServerRpcClient(grpcConn),
	}
	c.connPool[addr] = psConn

	return psConn, nil
}
