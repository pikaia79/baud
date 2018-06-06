package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/proto/pspb/raftpb"
	"github.com/tiglabs/baudengine/ps/storage"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
)

// Bulk perform many index/delete operations in a single API call.
func (s *Store) Bulk(requests []pspb.RequestUnion, timeout string) (responses []pspb.ResponseUnion, err error) {
	s.RLock()
	pstatus := s.Meta.Status
	s.RUnlock()
	if pstatus == metapb.PA_INVALID || pstatus == metapb.PA_NOTREAD {
		err = &metapb.PartitionNotFound{s.Meta.ID}
		return
	}

	var (
		timeCtx = s.Ctx
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
		future := s.RaftServer.Submit(s.Meta.ID, data)
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
	return s.fillBulkResponse(requests, result, err, done)
}

func (s *Store) fillBulkResponse(requests []pspb.RequestUnion, result interface{}, err error, done bool) ([]pspb.ResponseUnion, error) {
	if err == nil {
		return result.([]pspb.ResponseUnion), nil
	}

	switch err {
	case raft.ErrRaftNotExists:
		err = &metapb.PartitionNotFound{s.Meta.ID}

	case raft.ErrStopped:
		err = &metapb.ServerError{"the server is stopping, request is rejected"}

	case raft.ErrNotLeader:
		s.RLock()
		if s.Leader == 0 {
			err = &metapb.NoLeader{s.Meta.ID}
		} else {
			err = &metapb.NotLeader{
				PartitionID: s.Meta.ID,
				Leader:      metapb.NodeID(s.Leader),
				LeaderAddr:  s.LeaderAddr,
				Epoch:       s.Meta.Epoch,
			}
		}
		s.RUnlock()

	case context.DeadlineExceeded:
		err = storage.ErrorTimeout

	default:
		if done {
			err = &metapb.ServerError{"the server is shut down, request has proposed but not apply"}
		}
	}

	log.Error("bulk write document error: [%s]", err)

	return nil, err
}

func (s *Store) execRaftCommand(index uint64, cmds []pspb.RequestUnion) ([]pspb.ResponseUnion, error) {
	batch := s.Engine.NewWriteBatch()
	resp := make([]pspb.ResponseUnion, len(cmds))

	for i, cmd := range cmds {
		resp[i].OpType = cmd.OpType

		switch cmd.OpType {
		case pspb.OpType_CREATE:
			if createResp, err := s.createInternal(cmd.Create, batch); err == nil {
				resp[i].Create = createResp
			} else {
				log.Error("create document error:[%s],\n create request is:[%s]", err, cmd.Create)
				resp[i].Failure = &pspb.Failure{ID: cmd.Create.ID, Cause: err.Error()}
			}

		case pspb.OpType_UPDATE:
			if updateResp, err := s.updateInternal(cmd.Update, batch); err == nil {
				resp[i].Update = updateResp
			} else {
				log.Error("update document error:[%s],\n update request is:[%s]", err, cmd.Update)
				resp[i].Failure = &pspb.Failure{ID: cmd.Update.ID, Cause: err.Error()}
			}

		case pspb.OpType_DELETE:
			if delResp, err := s.deleteInternal(cmd.Delete, batch); err == nil {
				resp[i].Delete = delResp
			} else {
				log.Error("delete document error:[%s],\n delete request is:[%s]", err, cmd.Delete)
				resp[i].Failure = &pspb.Failure{ID: cmd.Delete.ID, Cause: err.Error()}
			}

		default:
			log.Error("unsupported command[%v]", cmd)
			resp[i].Failure = &pspb.Failure{Cause: storage.ErrorCommand.Error()}
		}
	}

	batch.SetApplyID(index)
	if err := batch.Commit(); err != nil {
		s.Engine.SetApplyID(index)
		log.Error("could not commit batch,error is:[%s]", err)
		return nil, errors.New("could not commit batch")
	}

	return resp, nil
}

func (s *Store) createInternal(request *pspb.CreateRequest, batch engine.Batch) (*pspb.CreateResponse, error) {
	var data interface{}
	if err := json.Unmarshal(request.Data, &data); err != nil {
		return nil, err
	}

	if err := batch.AddDocument(s.Ctx, engine.DOC_ID(request.ID), data); err != nil {
		return nil, err
	}

	return &pspb.CreateResponse{ID: request.ID, Result: pspb.WriteResult_CREATED}, nil
}

func (s *Store) updateInternal(request *pspb.UpdateRequest, batch engine.Batch) (*pspb.UpdateResponse, error) {
	var data interface{}
	if err := json.Unmarshal(request.Data, &data); err != nil {
		return nil, err
	}

	found, err := batch.UpdateDocument(s.Ctx, engine.DOC_ID(request.ID), data, request.Upsert)
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

func (s *Store) deleteInternal(request *pspb.DeleteRequest, batch engine.Batch) (*pspb.DeleteResponse, error) {
	n, err := batch.DeleteDocument(s.Ctx, engine.DOC_ID(request.ID))
	if err != nil {
		return nil, err
	}

	result := pspb.WriteResult_NOT_FOUND
	if n > 0 {
		result = pspb.WriteResult_DELETED
	}
	return &pspb.DeleteResponse{ID: request.ID, Result: result}, nil
}
