package router

import (
	"context"
	"github.com/pkg/errors"
	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/util/rpc"
	"google.golang.org/grpc"
)

type MasterClient struct {
	client     *rpc.Client
	masterAddr string
	context    context.Context
	cancel     context.CancelFunc
}

func NewMasterClient(masterAddr string) *MasterClient {
	mc := &MasterClient{masterAddr: masterAddr}
	connMgrOpt := rpc.DefaultManagerOption
	mc.context, mc.cancel = context.WithCancel(context.Background())
	connMgr := rpc.NewConnectionMgr(mc.context, &connMgrOpt)
	clientOpt := rpc.DefaultClientOption
	//clientOpt.ClusterID = serverConf.ClusterID
	clientOpt.ConnectMgr = connMgr
	clientOpt.CreateFunc = func(clientConn *grpc.ClientConn) interface{} { return masterpb.NewMasterRpcClient(clientConn) }
	mc.client = rpc.NewClient(1, &clientOpt)
	return mc
}

func (mc *MasterClient) GetRoute(dbId metapb.DBID, spaceId metapb.SpaceID, slotId metapb.SlotID) []masterpb.Route {
	request := &masterpb.GetRouteRequest{DB: dbId, Space: spaceId, Slot: slotId}
	ctx, cancel := mc.getContext()
	defer cancel()
	resp, err := mc.getClient().GetRoute(ctx, request)
	checkResponseOk(&resp.ResponseHeader, err)
	return resp.Routes
}

func (mc *MasterClient) GetDB(dbName string) metapb.DB {
	request := &masterpb.GetDBRequest{DBName: dbName}
	ctx, cancel := mc.getContext()
	defer cancel()
	resp, err := mc.getClient().GetDB(ctx, request)
	checkResponseOk(&resp.ResponseHeader, err)
	return resp.Db
}

func (mc *MasterClient) GetSpace(id metapb.DBID, spaceName string) metapb.Space {
	request := &masterpb.GetSpaceRequest{ID: id, SpaceName: spaceName}
	ctx, cancel := mc.getContext()
	defer cancel()
	resp, err := mc.getClient().GetSpace(ctx, request)
	checkResponseOk(&resp.ResponseHeader, err)
	return resp.Space
}

func (mc *MasterClient) getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(mc.context, rpcTimeoutDef)
}

func (mc *MasterClient) getClient() masterpb.MasterRpcClient {
	client, _ := mc.client.GetGrpcClient(mc.masterAddr)
	return client.(masterpb.MasterRpcClient)
}

func checkResponseOk(header *metapb.ResponseHeader, err error) bool {
	if err != nil {
		panic(err)
	}
	if header.Code != 0 {
		panic(errors.New(header.Message))
	}
	return true
}
