package server

import (
	"context"
	"fmt"
	"time"

	"github.com/tiglabs/baudengine/common/keys"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/pspb/raftpb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
)

// Get grpc handler of Get service
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

// BulkWrite grpc handler of BulkWrite service
func (s *Server) BulkWrite(ctx context.Context, request *pspb.BulkRequest) (*pspb.BulkResponse, error) {
	response := &pspb.BulkResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if !s.checkBulkWrite(request, response) {
		return response, nil
	}

	var (
		abort   string
		results = make(map[int]*raft.Future, len(request.Requests))
		cancel  context.CancelFunc
		timeCtx = s.ctx
	)
	if request.Timeout != "" {
		if timeout, err := time.ParseDuration(request.Timeout); err == nil {
			timeCtx, cancel = context.WithTimeout(timeCtx, timeout)
		}
	}
	response.Responses = make([]pspb.BulkItemResponse, len(request.Requests))
	for i, item := range request.Requests {
		response.Responses[i].OpType = item.OpType

		if abort == "" {
			raftCmd := raftpb.CreateRaftCommand()
			raftCmd.Type = raftpb.CmdType_WRITE
			raftCmd.WriteCommand = raftpb.CreateWriteCommand()
			raftCmd.WriteCommand.ContentType = request.ContentType
			raftCmd.WriteCommand.OpType = item.OpType
			raftCmd.WriteCommand.Index = item.Index
			raftCmd.WriteCommand.Update = item.Update
			raftCmd.WriteCommand.Delete = item.Delete

			if data, err := raftCmd.Marshal(); err != nil {
				response.Responses[i].Failure = &pspb.Failure{Id: getBulkRequestID(&item), Cause: err.Error()}
				log.Error("marshal raftCommand error, request is:[%v], \n error is:[%v]", item, err)
			} else {
				future := s.raftServer.Submit(request.PartitionID, data)
				_, subErr := future.AsyncResponse()
				select {
				case e := <-subErr:
					if e == raft.ErrRaftNotExists {
						abort = "partition not exists"
					} else if e == raft.ErrNotLeader {
						abort = "partition not leader"
					} else if e == raft.ErrStopped {
						abort = "server stopped"
					} else {
						response.Responses[i].Failure = &pspb.Failure{Id: getBulkRequestID(&item), Cause: e.Error()}
					}

				case <-timeCtx.Done():
					if timeCtx.Err() == context.DeadlineExceeded {
						abort = "request timeout"
					} else {
						abort = "server stopped"
					}

				default:
					results[i] = future
				}
			}
			raftCmd.Close()
		}

		if abort != "" {
			response.Responses[i].Failure = &pspb.Failure{Id: getBulkRequestID(&item), Cause: abort, Aborted: true}
		}
	}

	abort = ""
	for pos, future := range results {
		resp, respErr := future.AsyncResponse()
		if abort == "" {
			select {
			case <-timeCtx.Done():
				if timeCtx.Err() == context.DeadlineExceeded {
					abort = "request timeout"
				} else {
					abort = "server stopped"
				}

			case err := <-respErr:
				response.Responses[pos].Failure = &pspb.Failure{Id: getBulkRequestID(&request.Requests[pos]), Cause: err.Error()}

			case result := <-resp:
				fillBulkResponse(request, response, pos, result)
			}
		}

		if abort != "" {
			response.Responses[pos].Failure = &pspb.Failure{Id: getBulkRequestID(&request.Requests[pos]), Cause: abort}
		}
	}

	if cancel != nil {
		cancel()
	}

	return response, nil
}

func (s *Server) checkBulkWrite(request *pspb.BulkRequest, response *pspb.BulkResponse) bool {
	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"
		return false
	}

	p, ok := s.partitions.Load(request.PartitionID)
	if !ok {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.nodeID, request.PartitionID)
		return false
	}

	if err := p.(*partition).checkWritable(); err != nil {
		response.Error = *err
		if err.NotLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NOT_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] is not leader", s.nodeID, request.PartitionID)
		} else if err.NoLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NO_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has no leader", s.nodeID, request.PartitionID)
		} else if err.PartitionNotFound != nil {
			response.Code = metapb.PS_RESP_CODE_NO_PARTITION
			response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", s.nodeID, request.PartitionID)
		}

		return false
	}

	return true
}

func getBulkRequestID(request *pspb.BulkItemRequest) (id string) {
	switch request.OpType {
	case pspb.OpType_INDEX:
		id = ""
	case pspb.OpType_UPDATE:
		id = keys.EncodeDocIDToString(&request.Update.Id)
	case pspb.OpType_DELETE:
		id = keys.EncodeDocIDToString(&request.Delete.Id)
	}

	return
}

func fillBulkResponse(request *pspb.BulkRequest, response *pspb.BulkResponse, pos int, result interface{}) {
	switch request.Requests[pos].OpType {
	case pspb.OpType_INDEX:
		response.Responses[pos].Index = result.(*pspb.IndexResponse)
	case pspb.OpType_UPDATE:
		response.Responses[pos].Update = result.(*pspb.UpdateResponse)
	case pspb.OpType_DELETE:
		response.Responses[pos].Delete = result.(*pspb.DeleteResponse)
	}
}
