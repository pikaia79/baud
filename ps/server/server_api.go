package server

import (
	"context"
	"fmt"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
)

// Get grpc handler of Get service
func (s *Server) Get(ctx context.Context, request *pspb.GetRequest) (*pspb.GetResponse, error) {
	response := &pspb.GetResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
		Id: request.Id,
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "the server is stopping, request is rejected"
	} else if p, _ := s.partitions.Load(request.Partition); p == nil {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.nodeID, request.Partition)
		response.Error = metapb.Error{PartitionNotFound: &metapb.PartitionNotFound{request.Partition}}
	} else {
		p.(*partition).getInternal(request, response)
	}

	return response, nil
}

// BulkWrite grpc handler of BulkWrite service
func (s *Server) BulkWrite(ctx context.Context, request *pspb.BulkRequest) (*pspb.BulkResponse, error) {
	response := &pspb.BulkResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "the server is stopping, request is rejected"
	} else if p, _ := s.partitions.Load(request.Partition); p == nil {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.nodeID, request.Partition)
		response.Error = metapb.Error{PartitionNotFound: &metapb.PartitionNotFound{request.Partition}}
	} else {
		p.(*partition).bulkInternal(request, response)
	}

	return response, nil
}
