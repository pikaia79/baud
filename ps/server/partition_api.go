package server

import (
	"context"
	"errors"
	"time"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/pspb/raftpb"
	"github.com/tiglabs/baudengine/util/json"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
)

var timeoutErr = new(metapb.TimeoutError)

func (p *partition) getInternal(docID engine.DOC_ID, timeout string) (doc engine.DOCUMENT, found bool, err error) {
	if err = p.checkReadable(true); err != nil {
		log.Error("get document error: [%s]", err)
		return
	}

	var (
		timeCtx = p.ctx
		cancel  context.CancelFunc
	)
	if timeout != "" {
		if timeout, err := time.ParseDuration(timeout); err == nil {
			timeCtx, cancel = context.WithTimeout(timeCtx, timeout)
		}
	}
	doc, found = p.store.GetDocument(timeCtx, docID)
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
			err = timeoutErr
		} else {
			err = &metapb.ServerError{Cause: "during request processing, the server is shut down"}
		}
		log.Error("get document error: [%s]", err)
	}

	return
}

func (p *partition) bulkInternal(requests []pspb.RequestUnion, timeout string) (responses []pspb.ResponseUnion, err error) {
	p.rwMutex.RLock()
	pstatus := p.meta.Status
	p.rwMutex.RUnlock()
	if pstatus == metapb.PA_INVALID || pstatus == metapb.PA_NOTREAD {
		err = &metapb.PartitionNotFound{p.meta.ID}
		return
	}

	var (
		timeCtx = p.ctx
		cancel  context.CancelFunc

		done   bool
		result interface{}
	)
	if timeout != "" {
		if timeout, e := time.ParseDuration(timeout); e == nil {
			timeCtx, cancel = context.WithTimeout(timeCtx, timeout)
		}
	}

	raftCmd := raftpb.CreateRaftCommand()
	raftCmd.Type = raftpb.CmdType_WRITE
	raftCmd.WriteCommands = requests
	if data, e := raftCmd.Marshal(); e != nil {
		err = e
		log.Error("marshal raftCommand error: [%s]", err)
	} else {
		future := p.server.raftServer.Submit(p.meta.ID, data)
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
	return p.fillBulkResponse(requests, result, err, done)
}

func (p *partition) fillBulkResponse(requests []pspb.RequestUnion, result interface{}, err error, done bool) ([]pspb.ResponseUnion, error) {
	if err == nil {
		return result.([]pspb.ResponseUnion), nil
	}

	switch err {
	case raft.ErrRaftNotExists:
		err = &metapb.PartitionNotFound{p.meta.ID}

	case raft.ErrStopped:
		err = &metapb.ServerError{"the server is stopping, request is rejected"}

	case raft.ErrNotLeader:
		p.rwMutex.RLock()
		if p.leader == 0 {
			err = &metapb.NoLeader{p.meta.ID}
		} else {
			err = &metapb.NotLeader{
				PartitionID: p.meta.ID,
				Leader:      metapb.NodeID(p.leader),
				LeaderAddr:  p.leaderAddr,
				Epoch:       p.meta.Epoch,
			}
		}
		p.rwMutex.RUnlock()

	case context.DeadlineExceeded:
		err = timeoutErr

	default:
		if done {
			err = &metapb.ServerError{"the server is shut down, request has proposed but not apply"}
		}
	}

	log.Error("bulk write document error: [%s]", err)

	return nil, err
}

func (p *partition) execWriteCommand(index uint64, cmds []pspb.RequestUnion) ([]pspb.ResponseUnion, error) {
	batch := p.store.NewWriteBatch()
	resp := make([]pspb.ResponseUnion, len(cmds))

	for i, cmd := range cmds {
		resp[i].OpType = cmd.OpType

		switch cmd.OpType {
		case pspb.OpType_CREATE:
			if createResp, err := p.createInternal(cmd.Create, batch); err == nil {
				resp[i].Create = createResp
			} else {
				log.Error("create document error:[%s],\n create request is:[%s]", err, cmd.Create)
				resp[i].Failure = &pspb.Failure{ID: cmd.Create.ID, Cause: err.Error()}
			}

		case pspb.OpType_UPDATE:
			if updateResp, err := p.updateInternal(cmd.Update, batch); err == nil {
				resp[i].Update = updateResp
			} else {
				log.Error("update document error:[%s],\n update request is:[%s]", err, cmd.Update)
				resp[i].Failure = &pspb.Failure{ID: cmd.Update.ID, Cause: err.Error()}
			}

		case pspb.OpType_DELETE:
			if delResp, err := p.deleteInternal(cmd.Delete, batch); err == nil {
				resp[i].Delete = delResp
			} else {
				log.Error("delete document error:[%s],\n delete request is:[%s]", err, cmd.Delete)
				resp[i].Failure = &pspb.Failure{ID: cmd.Delete.ID, Cause: err.Error()}
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

func (p *partition) createInternal(request *pspb.CreateRequest, batch engine.Batch) (*pspb.CreateResponse, error) {
	var data interface{}
	if err := json.Unmarshal(request.Data, &data); err != nil {
		return nil, err
	}

	if err := batch.AddDocument(p.ctx, engine.DOC_ID(request.ID), data); err != nil {
		return nil, err
	}

	return &pspb.CreateResponse{ID: request.ID, Result: pspb.WriteResult_CREATED}, nil
}

func (p *partition) updateInternal(request *pspb.UpdateRequest, batch engine.Batch) (*pspb.UpdateResponse, error) {
	var data interface{}
	if err := json.Unmarshal(request.Data, &data); err != nil {
		return nil, err
	}

	found, err := batch.UpdateDocument(p.ctx, engine.DOC_ID(request.ID), data, request.Upsert)
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if found {
		result = pspb.WriteResult_UPDATED
	} else if request.Upsert {
		result = pspb.WriteResult_CREATED
	}
	return &pspb.UpdateResponse{ID: request.ID, Result: result}, nil
}

func (p *partition) deleteInternal(request *pspb.DeleteRequest, batch engine.Batch) (*pspb.DeleteResponse, error) {
	n, err := batch.DeleteDocument(p.ctx, engine.DOC_ID(request.ID))
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if n > 0 {
		result = pspb.WriteResult_DELETED
	}
	return &pspb.DeleteResponse{ID: request.ID, Result: result}, nil
}
