package master

import (
    "sync"
    "google.golang.org/grpc"
    "proto/metapb"
    "proto/pspb"
    "util/log"
    "time"
    "context"
    "google.golang.org/grpc/status"
)

const (
    GRPC_REQUEST_TIMEOUT = time.Second
    GRPC_CONN_TIMEOUT    = time.Second * 3
)

var (
    singleInstance *PSClient
    instanceLock   sync.RWMutex
)

type PSClient struct {
    connPool map[string]*PSConn
    lock     sync.RWMutex
}

func GetPSRpcClientInstance() *PSClient {
    if singleInstance != nil {
        return singleInstance
    }

    instanceLock.Lock()
    defer instanceLock.Unlock()
    if singleInstance == nil {
        singleInstance = &PSClient{
            connPool: make(map[string]*PSConn),
        }
    }
    return singleInstance
}

type PSConn struct {
    rpcAddr string
    conn    *grpc.ClientConn
    client  pspb.ApiGrpcClient
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

        header = out.ResponseHeader
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

func (client *PSClient) CreateReplica(addr string, partition *metapb.Partition) error {
    psConn, err := client.getConn(addr)
    if err != nil {
        return err
    }

    req := &pspb.CreatePartitionRequest{
        RequestHeader: new(metapb.RequestHeader),
        Partition:     partition,
    }
    _, err = psConn.callRpc(req, GRPC_REQUEST_TIMEOUT)
    if err != nil {
        return err
    }

    return nil
}

func (client *PSClient) DeleteReplica(addr string, partition *metapb.Partition) error {
    psConn, err := client.getConn(addr)
    if err != nil {
        return err
    }

    req := &pspb.DeletePartitionRequest{
        RequestHeader: new(metapb.RequestHeader),
        Partition:     partition,
    }
    _, err = psConn.callRpc(req, GRPC_REQUEST_TIMEOUT)
    if err != nil {
        return err
    }

    return nil
}

func (client *PSClient) getConn(addr string) (*PSConn, error) {
    if len(addr) == 0 {
        return nil, ErrInternalError
    }
    client.lock.Lock()
    defer client.lock.Unlock()

    if psConn, ok := client.connPool[addr]; ok {
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
    client.connPool[addr] = psConn

    return psConn, nil
}
