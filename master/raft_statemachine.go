package master

import (
	"context"
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
	"proto/ms_raftcmdpb"
	"util/log"
	"util/raftkvstore"
)

var (
	errNoApplyHandler         = errors.New("raft group not register apply handler")
	errNoPeerChangeHandler    = errors.New("raft group not register peer change handler")
	errNoLeaderChangeHandler  = errors.New("raft group not register leader change handler")
	errNoFatalEventHandler    = errors.New("raft group not register fatal event handler")
	errNoSnapshotHandler      = errors.New("raft group not register snapshot handler")
	errNoApplySnapshotHandler = errors.New("raft group not register apply snapshot handler")

	errUnknownResponseType = errors.New("unknown repsonse type")
)

type RaftApplyHandler func( /*req*/ *ms_raftcmdpb.Request, uint64) ( /*resp*/ *ms_raftcmdpb.Response /*err*/, error)

type RaftPeerChangeHandler func( /*confChange*/ *raftproto.ConfChange) ( /*res*/ interface{} /*err*/, error)

type RaftLeaderChangeHandler func( /*leader*/ uint64)

type RaftFatalEventHandler func( /*err*/ *raft.FatalError)

type RaftGetSnapshotHandler func() (raftkvstore.Snapshot, error)

type RaftApplySnapshotHandler func([]raftproto.Peer, *SnapshotKVIterator) error

type RaftGroup struct {
	id       uint64
	startKey []byte
	endKey   []byte

	raftServer *raft.RaftServer

	raftApplyHandle         RaftApplyHandler
	raftPeerChangeHandle    RaftPeerChangeHandler
	raftLeaderChangeHandle  RaftLeaderChangeHandler
	raftFatalEventHandle    RaftFatalEventHandler
	raftGetSnapshotHandle   RaftGetSnapshotHandler
	raftApplySnapshotHandle RaftApplySnapshotHandler
}

func NewRaftGroup(groupId uint64, raftServer *raft.RaftServer, startKey, endKey []byte) *RaftGroup {
	rg := &RaftGroup{
		id:         groupId,
		startKey:   startKey,
		endKey:     endKey,
		raftServer: raftServer,
	}

	return rg
}

func (rg *RaftGroup) Start(raftConfig *raft.RaftConfig) error {
	if rg.raftApplyHandle == nil || rg.raftApplySnapshotHandle == nil ||
		rg.raftFatalEventHandle == nil || rg.raftPeerChangeHandle == nil ||
		rg.raftGetSnapshotHandle == nil || rg.raftLeaderChangeHandle == nil {
		return errors.New("register raft handler first")
	}

	err := rg.raftServer.CreateRaft(raftConfig)
	if err != nil {
		log.Error("create raft group failed, err[%v]", err)
		return err
	}
	return nil
}

func (rg *RaftGroup) Release() error {
	return rg.raftServer.RemoveRaft(rg.id)
}

func (rg *RaftGroup) submit(ctx context.Context, cmd []byte) (interface{}, error) {
	// TODO:ctx
	future := rg.raftServer.Submit(rg.id, cmd)
	resp, err := future.Response()
	if err != nil {
		return nil, err
	}

	if err, ok := resp.(error); ok {
		return nil, err
	}

	return resp, nil
}

func (rg *RaftGroup) SubmitCommand(ctx context.Context, req *ms_raftcmdpb.Request) (*ms_raftcmdpb.Response, error) {
	cmd, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := rg.submit(ctx, cmd)
	if err != nil {
		return nil, err
	}
	if rsp, ok := resp.(*ms_raftcmdpb.Response); ok {
		return rsp, nil
	}
	return nil, errUnknownResponseType
}

/////////////////////////callback begin////////////////////////////////
func (rg *RaftGroup) RegisterApplyHandle(handler RaftApplyHandler) {
	rg.raftApplyHandle = handler
}

func (rg *RaftGroup) RegisterPeerChangeHandle(handler RaftPeerChangeHandler) {
	rg.raftPeerChangeHandle = handler
}

func (rg *RaftGroup) RegisterGetSnapshotHandle(handler RaftGetSnapshotHandler) {
	rg.raftGetSnapshotHandle = handler
}

func (rg *RaftGroup) RegisterApplySnapshotHandle(handler RaftApplySnapshotHandler) {
	rg.raftApplySnapshotHandle = handler
}

func (rg *RaftGroup) RegisterLeaderChangeHandle(handler RaftLeaderChangeHandler) {
	rg.raftLeaderChangeHandle = handler
}

func (rg *RaftGroup) RegisterFatalEventHandle(handler RaftFatalEventHandler) {
	rg.raftFatalEventHandle = handler
}

/////////////////////////callback end////////////////////////////////

//////////////////////statemachine begin///////////////////
func (rg *RaftGroup) Apply(command []byte, index uint64) (res interface{}, err error) {
	//TODO: PUT/DELTE & StoreApplyIndex use WriteBatch?
	cmd := &ms_raftcmdpb.Request{}
	err = proto.Unmarshal(command, cmd)
	if err != nil {
		return
	}
	if rg.raftApplyHandle != nil {
		res, err = rg.raftApplyHandle(cmd, index)
	} else {
		err = errNoApplyHandler
	}

	return
}

func (rg *RaftGroup) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (res interface{}, err error) {
	if rg.raftPeerChangeHandle != nil {
		res, err = rg.raftPeerChangeHandle(confChange)
	} else {
		err = errNoPeerChangeHandler
	}

	return
}

// TODO raft index with config command
func (rg *RaftGroup) Snapshot() (raftproto.Snapshot, error) {
	if rg.raftGetSnapshotHandle != nil {
		snap, err := rg.raftGetSnapshotHandle()
		if err != nil {
			return nil, err
		}
		applyIndex := snap.ApplyIndex()
		return NewRaftSnapshot(snap, applyIndex, []byte(rg.startKey), []byte(rg.endKey)), nil
	}
	log.Error("not register snap handler")
	return nil, errNoSnapshotHandler
}

func (rg *RaftGroup) ApplySnapshot(peers []raftproto.Peer, rawIter raftproto.SnapIterator) error {
	iter := NewSnapshotKVIterator(rawIter)
	if rg.raftApplySnapshotHandle != nil {
		return rg.raftApplySnapshotHandle(peers, iter)
	}
	log.Error("not register apply snap handler")
	return errNoApplySnapshotHandler
}

func (rg *RaftGroup) HandleLeaderChange(leader uint64) {
	if rg.raftLeaderChangeHandle != nil {
		rg.raftLeaderChangeHandle(leader)
	} else {
		log.Warn("raft group not register leader change handler")
	}
}

func (rg *RaftGroup) HandleFatalEvent(err *raft.FatalError) {
	if rg.raftFatalEventHandle != nil {
		rg.raftFatalEventHandle(err)
	} else {
		log.Warn("raft group not register fatal event handler")
	}
}

//////////////////////statemachine end///////////////////
