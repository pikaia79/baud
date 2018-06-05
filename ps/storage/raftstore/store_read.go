package raftstore

import (
	"context"
	"time"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/ps/storage"
	"github.com/tiglabs/baudengine/util/log"
)

// Get get the document according to the specified id
func (s *Store) Get(docID engine.DOC_ID, timeout string) (doc engine.DOCUMENT, found bool, err error) {
	if err = s.checkReadable(true); err != nil {
		log.Error("get document error: [%s]", err)
		return
	}

	var (
		timeCtx = s.Ctx
		cancel  context.CancelFunc
	)
	if timeout != "" {
		if timeout, err := time.ParseDuration(timeout); err == nil {
			timeCtx, cancel = context.WithTimeout(timeCtx, timeout)
		}
	}
	doc, found = s.Engine.GetDocument(timeCtx, docID)
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
			err = storage.ErrorTimeout
		} else {
			err = &metapb.ServerError{Cause: "during request processing, the server is shut down"}
		}
		log.Error("get document error: [%s]", err)
	}

	return
}

func (s *Store) checkReadable(readLeader bool) (err error) {
	s.RLock()

	if s.Meta.Status == metapb.PA_INVALID || s.Meta.Status == metapb.PA_NOTREAD {
		err = &metapb.PartitionNotFound{s.Meta.ID}
		goto ret
	}
	if s.Leader == 0 {
		err = &metapb.NoLeader{s.Meta.ID}
		goto ret
	}
	if readLeader && s.Leader != uint64(s.Server.NodeID) {
		err = &metapb.NotLeader{
			PartitionID: s.Meta.ID,
			Leader:      metapb.NodeID(s.Leader),
			LeaderAddr:  s.LeaderAddr,
			Epoch:       s.Meta.Epoch,
		}
		goto ret
	}

ret:
	s.RUnlock()
	return
}
