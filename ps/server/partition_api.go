package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tiglabs/baudengine/kernel"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/pspb/raftpb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
)

func (p *partition) getInternal(request *pspb.GetRequest, response *pspb.GetResponse) {
	if err := p.checkReadable(true); err != nil {
		response.Error = *err
		if err.NotLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NOT_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] is not leader", p.server.NodeID, request.Partition)
		} else if err.NoLeader != nil {
			response.Code = metapb.PS_RESP_CODE_NO_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has no leader", p.server.NodeID, request.Partition)
		} else if err.PartitionNotFound != nil {
			response.Code = metapb.PS_RESP_CODE_NO_PARTITION
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has closed", p.server.NodeID, request.Partition)
		}

		log.Error("get document error:[%s],\n get request is:[%s]", response.Message, request)
		return
	}

	var (
		err     error
		fields  map[uint32]pspb.FieldValue
		cancel  context.CancelFunc
		timeCtx = p.ctx
	)
	if request.Timeout != "" {
		if timeout, err := time.ParseDuration(request.Timeout); err == nil {
			timeCtx, cancel = context.WithTimeout(timeCtx, timeout)
		}
	}
	fields, response.Found = p.store.GetDocument(timeCtx, request.Id, request.Fields)
	select {
	case <-timeCtx.Done():
		err = timeCtx.Err()
	default:
		if cancel != nil {
			cancel()
		}
	}

	if err != nil {
		if err == context.DeadlineExceeded {
			response.Code = metapb.RESP_CODE_TIMEOUT
			response.Message = "request timeout"
		} else {
			response.Code = metapb.RESP_CODE_SERVER_STOP
			response.Message = "during request processing, the server is shut down"
		}
		log.Error("get document error:[%s],\n get request is:[%s]", err, request)
	} else if response.Found && len(fields) > 0 {
		response.Fields = fields
	}
	return
}

func (p *partition) bulkInternal(request *pspb.BulkRequest, response *pspb.BulkResponse) {
	p.rwMutex.RLock()
	pstatus := p.meta.Status
	p.rwMutex.RUnlock()
	if pstatus == metapb.PA_INVALID || pstatus == metapb.PA_NOTREAD {
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", p.server.NodeID, request.Partition)
		response.Error = metapb.Error{PartitionNotFound: &metapb.PartitionNotFound{request.Partition}}
		return
	}

	var (
		timeCtx = p.ctx
		cancel  context.CancelFunc

		err    error
		done   bool
		result interface{}
	)
	if request.Timeout != "" {
		if timeout, e := time.ParseDuration(request.Timeout); e == nil {
			timeCtx, cancel = context.WithTimeout(timeCtx, timeout)
		}
	}

	raftCmd := raftpb.CreateRaftCommand()
	raftCmd.Type = raftpb.CmdType_WRITE
	raftCmd.WriteCommands = request.Requests
	if data, e := raftCmd.Marshal(); e != nil {
		err = e
		log.Error("marshal raftCommand error:[%s],\n request is:[%s]", err, request)
	} else {
		future := p.server.raftServer.Submit(request.Partition, data)
		respCh, errCh := future.AsyncResponse()
		raftCmd.Close()

		select {
		case <-timeCtx.Done():
			err = timeCtx.Err()
			done = true

		case err = <-errCh:

		case result = <-respCh:
		}
	}

	if cancel != nil {
		cancel()
	}
	p.fillBulkResponse(request, response, result, err, done)
}

func (p *partition) fillBulkResponse(request *pspb.BulkRequest, response *pspb.BulkResponse, result interface{}, err error, done bool) {
	if err == nil {
		response.Responses = result.([]pspb.BulkItemResponse)
		return
	}

	switch err {
	case raft.ErrRaftNotExists:
		response.Code = metapb.PS_RESP_CODE_NO_PARTITION
		response.Message = fmt.Sprintf("node[%d] has not found partition[%d]", p.server.NodeID, request.Partition)
		response.Error = metapb.Error{PartitionNotFound: &metapb.PartitionNotFound{request.Partition}}

	case raft.ErrStopped:
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "the server is stopping, request is rejected"

	case raft.ErrNotLeader:
		p.rwMutex.RLock()
		if p.leader == 0 {
			response.Code = metapb.PS_RESP_CODE_NO_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] has no leader", p.server.NodeID, request.Partition)
			response.Error = metapb.Error{NoLeader: &metapb.NoLeader{request.Partition}}
		} else {
			response.Code = metapb.PS_RESP_CODE_NOT_LEADER
			response.Message = fmt.Sprintf("node[%d] of partition[%d] is not leader", p.server.NodeID, request.Partition)
			response.Error = metapb.Error{NotLeader: &metapb.NotLeader{
				PartitionID: request.Partition,
				Leader:      metapb.NodeID(p.leader),
				LeaderAddr:  p.leaderAddr,
				Epoch:       p.meta.Epoch,
			}}
		}
		p.rwMutex.RUnlock()

	case context.DeadlineExceeded:
		response.Code = metapb.RESP_CODE_TIMEOUT
		response.Message = "request timeout"

	default:
		if done {
			response.Code = metapb.RESP_CODE_SERVER_STOP
			response.Message = "the server is shut down, request has proposed but not apply"
		} else {
			response.Code = metapb.RESP_CODE_SERVER_ERROR
			response.Message = err.Error()
		}
	}

	log.Error("bulk write document error:[%s],\n bulk request is:[%s]", response.Message, request)
}

func (p *partition) execWriteCommand(index uint64, cmds []pspb.BulkItemRequest) ([]pspb.BulkItemResponse, error) {
	batch := p.store.NewWriteBatch()
	resp := make([]pspb.BulkItemResponse, len(cmds))

	for i, cmd := range cmds {
		resp[i].OpType = cmd.OpType

		switch cmd.OpType {
		case pspb.OpType_CREATE:
			if createResp, err := p.createInternal(cmd.Create, batch); err == nil {
				resp[i].Create = createResp
			} else {
				log.Error("create document error:[%s],\n create request is:[%s]", err, cmd.Create)
				resp[i].Failure = &pspb.Failure{Id: cmd.Create.Doc.Id, Cause: err.Error()}
			}

		case pspb.OpType_UPDATE:
			if updateResp, err := p.updateInternal(cmd.Update, batch); err == nil {
				resp[i].Update = updateResp
			} else {
				log.Error("update document error:[%s],\n update request is:[%s]", err, cmd.Update)
				resp[i].Failure = &pspb.Failure{Id: cmd.Update.Doc.Id, Cause: err.Error()}
			}

		case pspb.OpType_DELETE:
			if delResp, err := p.deleteInternal(cmd.Delete, batch); err == nil {
				resp[i].Delete = delResp
			} else {
				log.Error("delete document error:[%s],\n delete request is:[%s]", err, cmd.Delete)
				resp[i].Failure = &pspb.Failure{Id: cmd.Delete.Id, Cause: err.Error()}
			}

		default:
			log.Error("unsupported command[%v]", cmd)
			resp[i].Failure = &pspb.Failure{Cause: errorPartitonCommand.Error()}
		}
	}

	batch.SetApplyID(index)
	if err := batch.Commit(); err != nil {
		p.store.SetApplyID(index)
		log.Error("could not commit batch,error is:[%s]", err)
		return nil, errors.New("could not commit batch")
	}

	return resp, nil
}

func (p *partition) createInternal(request *pspb.CreateRequest, batch kernel.Batch) (*pspb.CreateResponse, error) {
	if err := batch.AddDocument(p.ctx, &request.Doc); err != nil {
		return nil, err
	}

	return &pspb.CreateResponse{Id: request.Doc.Id, Result: pspb.WriteResult_CREATED}, nil
}

func (p *partition) updateInternal(request *pspb.UpdateRequest, batch kernel.Batch) (*pspb.UpdateResponse, error) {
	found, err := batch.UpdateDocument(p.ctx, &request.Doc, request.Upsert)
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if found {
		result = pspb.WriteResult_UPDATED
	} else if request.Upsert {
		result = pspb.WriteResult_CREATED
	}
	return &pspb.UpdateResponse{Id: request.Doc.Id, Result: result}, nil
}

func (p *partition) deleteInternal(request *pspb.DeleteRequest, batch kernel.Batch) (*pspb.DeleteResponse, error) {
	n, err := batch.DeleteDocument(p.ctx, request.Id)
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if n > 0 {
		result = pspb.WriteResult_DELETED
	}
	return &pspb.DeleteResponse{Id: request.Id, Result: result}, nil
}
