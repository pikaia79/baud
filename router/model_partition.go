package router

import (
	"context"
	"errors"
	"github.com/tiglabs/baud/keys"
	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
	"github.com/tiglabs/baud/util/rpc"
	"google.golang.org/grpc"
)

type Partition struct {
	meta          metapb.Partition
	route         masterpb.Route
	parent        *Space
	psClient      *rpc.Client
	leaderAddr    string
	requestHeader pspb.ActionRequestHeader
}

func NewPartition(parent *Space, route masterpb.Route) *Partition {
	partition := &Partition{meta: route.Partition, parent: parent, route: route}
	connMgrOpt := rpc.DefaultManagerOption
	connMgr := rpc.NewConnectionMgr(parent.parent.context, &connMgrOpt)
	clientOpt := rpc.DefaultClientOption
	//clientOpt.ClusterID = serverConf.ClusterID
	clientOpt.ConnectMgr = connMgr
	clientOpt.CreateFunc = func(clientConn *grpc.ClientConn) interface{} { return pspb.NewApiGrpcClient(clientConn) }
	partition.psClient = rpc.NewClient(1, &clientOpt)
	return partition
}

func (partition *Partition) Create(docBody []byte) *metapb.DocID {
	createReq := pspb.BulkItemRequest{
		OpType: pspb.OpType_CREATE,
		Index:  &pspb.IndexRequest{OpType: pspb.OpType_CREATE, Source: docBody},
	}
	request := &pspb.BulkRequest{
		ActionRequestHeader: partition.requestHeader,
		Requests:            []pspb.BulkItemRequest{createReq},
	}
	ctx, cancel := partition.getContext()
	defer cancel()
	resp := partition.getSingleResponse(partition.getClient().BulkWrite(ctx, request)).Index
	docId, err := keys.DecodeDocIDFromString(resp.Id)
	if err != nil {
		panic(err)
	}
	return docId
}

func (partition *Partition) Read(docId *metapb.DocID) metapb.Value {
	request := &pspb.GetRequest{ActionRequestHeader: partition.requestHeader, Id:*docId}
	ctx, cancel := partition.getContext()
	defer cancel()
	resp, err := partition.getClient().Get(ctx, request)
	if err != nil {
		panic(err)
	}
	return resp.Fields
}

func (partition *Partition) Update(docId *metapb.DocID, docBody []byte) {
	createReq := pspb.BulkItemRequest{
		OpType: pspb.OpType_UPDATE,
		Update: &pspb.UpdateRequest{Id: *docId, Doc: docBody},
	}
	request := &pspb.BulkRequest{
		ActionRequestHeader: partition.requestHeader,
		Requests:            []pspb.BulkItemRequest{createReq},
	}
	ctx, cancel := partition.getContext()
	defer cancel()
	resp := partition.getSingleResponse(partition.getClient().BulkWrite(ctx, request)).Index
	docId, err := keys.DecodeDocIDFromString(resp.Id)
	if err != nil {
		panic(err)
	}
}

func (partition *Partition) Delete(docId *metapb.DocID) bool {
	deleteReq := pspb.BulkItemRequest{
		OpType: pspb.OpType_DELETE,
		Delete: &pspb.DeleteRequest{Id: *docId},
	}
	request := &pspb.BulkRequest{
		ActionRequestHeader: partition.requestHeader,
		Requests:            []pspb.BulkItemRequest{deleteReq},
	}
	ctx, cancel := partition.getContext()
	defer cancel()
	resp := partition.getSingleResponse(partition.getClient().BulkWrite(ctx, request)).Delete
	if resp.Result != pspb.WriteResult_DELETED {
		return false
	}
	return true
}

func (partition *Partition) getClient() pspb.ApiGrpcClient {
	psClient, _ := partition.psClient.GetGrpcClient(partition.leaderAddr)
	return psClient.(pspb.ApiGrpcClient)
}

func (partition *Partition) getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(partition.parent.parent.context, rpcTimeoutDef)
}

func (partition *Partition) getSingleResponse(resp *pspb.BulkResponse, err error) *pspb.BulkItemResponse {
	if err != nil {
		panic(err)
	}
	if resp.Code != 0 {
		panic(errors.New(resp.Message))
	}
	if len(resp.Responses) != 1 || resp.Responses[0].OpType != pspb.OpType_CREATE {
		panic(errors.New("bad response for OpType_CREATE"))
	}
	return &resp.Responses[0]
}
