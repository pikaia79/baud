package server

import (
	"context"
	"fmt"

	"github.com/tiglabs/baudengine/common/keys"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
)

func (s *Server) Get(ctx context.Context, request *pspb.GetRequest) (*pspb.GetResponse, error) {
	response := &pspb.GetResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
		Id: keys.EncodeDocIDToString(&request.Id),
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"

		return response, nil
	}

	if p, ok := s.partitions.Load(request.PartitionID); ok {
		p.(*partition).getInternal(request, response)
	} else {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.nodeID, request.PartitionID)
	}

	return response, nil
}

func (s *Server) BulkWrite(ctx context.Context, request *pspb.BulkRequest) (*pspb.BulkResponse, error) {
	response := &pspb.BulkResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"

		return response, nil
	}

	if p, ok := s.partitions.Load(request.PartitionID); ok {
		p.(*partition).bulkInternal(request, response)
	} else {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.nodeID, request.PartitionID)
	}

	return response, nil
}
