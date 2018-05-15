package master

import (
	"fmt"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/raftkvstore"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"golang.org/x/net/context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

//go:generate mockgen -destination raft_store_mock.go -package master github.com/tiglabs/baudengine/master Store,Batch
//go:generate mockgen -destination store_mock.go -package master github.com/tiglabs/baudengine/util/raftkvstore Iterator

const (
	FIXED_RAFTGROUPID          = 1
	DEFAULT_RAFTLOG_LIMIT      = 10000
	DEFAULT_SUBMIT_TIMEOUT_MAX = time.Second * 60
)

var (
	raftBucket = []byte("MasterRaftBucket")
	dbBucket   = []byte("MasterDbBucket")
)

////////////////////store begin///////////////////
type Store interface {
	// TODO: all methods need to add parameter timeout
	Open() error
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Scan(startKey, limitKey []byte) raftkvstore.Iterator
	NewBatch() Batch
    GetLeaderAsync(leaderChangingCh chan *LeaderInfo)
	GetLeaderSync() *LeaderInfo
    Close() error
}

type Batch interface {
	Put(key []byte, value []byte)
	Delete(key []byte)

	Commit() error
}

type RaftStore struct {
    config           *Config
    localStore       raftkvstore.Store
    localRead        bool
    leaderChangingCh chan *LeaderInfo
    leaderInfo       *LeaderInfo

    raftStoreConfig *RaftStoreConfig
    raftGroup       *RaftGroup
    raftServer      *raft.RaftServer
    raftConfig      *raft.RaftConfig

    ctx       context.Context
    ctxCancel context.CancelFunc
    wg        sync.WaitGroup
}

type Peer struct {
	NodeId uint64
	RaftHeartbeatAddr string
	RaftReplicateAddr string
}

type RaftStoreConfig struct {
	RaftRetainLogs        uint64
	RaftHeartbeatInterval time.Duration
	RaftHeartbeatAddr     string
	RaftReplicateAddr     string
	RaftNodes             []*Peer

	NodeId   uint64
	DataPath string
	WalPath  string
}

type LeaderInfo struct {
    becomeLeader   bool
    newLeaderId    uint64  // 0: no leader
    newLeaderAddr  string
}

func NewRaftStore(config *Config) *RaftStore {
	rs := &RaftStore{
		config:         config,
		localRead:      true,
	}
	rs.ctx, rs.ctxCancel = context.WithCancel(context.Background())
	return rs
}

func (rs *RaftStore) Open() error {
	if err := rs.initRaftStoreCfg(); err != nil {
		return err
	}
	if err := rs.initRaftServer(); err != nil {
		return err
	}

	if err := rs.raftGroup.Start(rs.raftConfig); err != nil {
		return err
	}
	rs.wg.Add(1)
	go rs.raftLogCleanup()

	log.Info("Raft store has started")
	return nil
}

func (rs *RaftStore) Put(key, value []byte) error {
	req := &masterpb.Request{
		CmdType: masterpb.CmdType_Put,
		PutReq: &masterpb.RaftPutRequest{
			Key:   key,
			Value: value,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DEFAULT_SUBMIT_TIMEOUT_MAX)
	defer cancel()
	_, err := rs.raftGroup.SubmitCommand(ctx, req)
	if err != nil {
		log.Error("raft submit failed, err[%v]", err)
		return err
	}
	return nil
}

func (rs *RaftStore) Delete(key []byte) error {
	req := &masterpb.Request{
		CmdType: masterpb.CmdType_Delete,
		DeleteReq: &masterpb.RaftDeleteRequest{
			Key: key,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DEFAULT_SUBMIT_TIMEOUT_MAX)
	defer cancel()
	_, err := rs.raftGroup.SubmitCommand(ctx, req)
	if err != nil {
		log.Error("raft submit failed, err[%v]", err)
		return err
	}
	return nil
}

func (rs *RaftStore) Get(key []byte) ([]byte, error) {
	if rs.localRead {
		return rs.localStore.Get(key)
	}
	req := &masterpb.Request{
		CmdType: masterpb.CmdType_Get,
		GetReq: &masterpb.RaftGetRequest{
			Key: key,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DEFAULT_SUBMIT_TIMEOUT_MAX)
	defer cancel()
	resp, err := rs.raftGroup.SubmitCommand(ctx, req)
	if err != nil {
		return nil, err
	}
	value := resp.GetResp.Value
	return value, nil
}

func (rs *RaftStore) Scan(startKey, limitKey []byte) raftkvstore.Iterator {
	return rs.localStore.NewIterator(startKey, limitKey)
}

func (rs *RaftStore) NewBatch() Batch {
	return NewSaveBatch(rs.raftGroup)
}

func (rs *RaftStore) GetLeaderAsync(leaderChangingCh chan *LeaderInfo) {
	rs.leaderChangingCh = leaderChangingCh
}

func (rs *RaftStore) GetLeaderSync() *LeaderInfo {
    return rs.leaderInfo
}

func (rs *RaftStore) Close() error {
	var lastErr error
	if rs.raftGroup != nil {
		if err := rs.raftGroup.Release(); err != nil {
			log.Error("fail to close raftgroup. err:[%v]", err)
			lastErr = err
		}
	}

	if rs.ctxCancel != nil {
		rs.ctxCancel()
	}
	rs.wg.Wait()

	if rs.localStore != nil {
		if err := rs.localStore.Close(); err != nil {
			log.Error("fail to close boltdb store. err:[%v]", err)
			lastErr = err
		}
	}

	log.Info("Raft store has closed")
	return lastErr
}

type SaveBatch struct {
	raft  *RaftGroup
	lock  sync.RWMutex
	batch []*masterpb.KvPairExecute
}

func NewSaveBatch(raft *RaftGroup) *SaveBatch {
	return &SaveBatch{raft: raft, batch: nil}
}

func (b *SaveBatch) Put(key, value []byte) {
	destKey := make([]byte, len(key))
	destVal := make([]byte, len(value))
	copy(destKey, key)
	copy(destVal, value)
	exec := &masterpb.KvPairExecute{
		Do:     masterpb.ExecuteType_ExecPut,
		KvPair: &masterpb.KvPair{Key: destKey, Value: destVal},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Delete(key []byte) {
	destKey := make([]byte, len(key))
	copy(destKey, key)
	exec := &masterpb.KvPairExecute{
		Do:     masterpb.ExecuteType_ExecDelete,
		KvPair: &masterpb.KvPair{Key: destKey},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Commit() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	batch := b.batch
	b.batch = nil
	// 空提交
	if len(batch) == 0 {
		return nil
	}
	req := &masterpb.Request{
		CmdType: masterpb.CmdType_Execute,
		ExecuteReq: &masterpb.ExecuteRequest{
			Execs: batch,
		},
	}
	_, err := b.raft.SubmitCommand(context.Background(), req)
	if err != nil {
		// TODO error
		log.Error("raft submit failed, err[%v]", err.Error())
		return err
	}
	return nil
}
/////////////////////store end////////////////////////

////////////////////raft begin////////////////////////
func (rs *RaftStore) initRaftStoreCfg() error {
	raftStoreCfg := new(RaftStoreConfig)
	raftStoreCfg.NodeId = rs.config.ClusterCfg.CurNode.NodeId

	raftDataDir := filepath.Join(rs.config.ModuleCfg.DataPath, "raft")
	if err := os.MkdirAll(raftDataDir, 0755); err != nil {
		log.Error("make raft data root directory[%v] failed, err[%v]", raftDataDir, err)
		return err
	}
	raftStoreCfg.DataPath = filepath.Join(rs.config.ModuleCfg.DataPath, "baudengine.db")
	raftStoreCfg.WalPath = raftDataDir

	raftStoreCfg.RaftRetainLogs = rs.config.ClusterCfg.RaftRetainLogsCount
	raftStoreCfg.RaftHeartbeatInterval = time.Duration(rs.config.ClusterCfg.RaftHeartbeatInterval) * time.Millisecond
	raftStoreCfg.RaftHeartbeatAddr = util.BuildAddr(rs.config.ClusterCfg.CurNode.Host,
		rs.config.ClusterCfg.CurNode.RaftHeartbeatPort)
	raftStoreCfg.RaftReplicateAddr = util.BuildAddr(rs.config.ClusterCfg.CurNode.Host,
		rs.config.ClusterCfg.CurNode.RaftReplicatePort)

	var peers []*Peer
	for _, p := range rs.config.ClusterCfg.Nodes {
		peer := new(Peer)
		peer.NodeId = p.NodeId
		peer.RaftHeartbeatAddr = util.BuildAddr(p.Host, p.RaftHeartbeatPort)
		peer.RaftReplicateAddr = util.BuildAddr(p.Host, p.RaftReplicatePort)
		peers = append(peers, peer)
	}
	raftStoreCfg.RaftNodes = peers

	rs.raftStoreConfig = raftStoreCfg

	return nil
}

func (rs *RaftStore) initRaftServer() error {
	cfg := rs.raftStoreConfig

	rc := raft.DefaultConfig()
	rc.RetainLogs = cfg.RaftRetainLogs
	rc.TickInterval = cfg.RaftHeartbeatInterval
	rc.HeartbeatAddr = cfg.RaftHeartbeatAddr
	rc.ReplicateAddr = cfg.RaftReplicateAddr
	rc.Resolver = NewResolver(cfg.RaftNodes)
	rc.NodeID = cfg.NodeId
	raftServer, err := raft.NewRaftServer(rc)
	if err != nil {
		log.Error("new raft server failed. err:[%v]", err)
		return err
	}

	rowStore, applyId, err := raftkvstore.NewBoltStore(dbBucket, raftBucket, cfg.DataPath)
	if err != nil {
		log.Error("open bolt localStore failed. err[%v]", err)
		return err
	}

	// TODO: package wal of tiglabs/raft has no param 'id', need to update
	walStore, err := wal.NewStorage(cfg.WalPath, nil)
	if err != nil {
		log.Error("create raft wal storage failed. err:[%v]", err)
		return err
	}

	raftGroup := NewRaftGroup(FIXED_RAFTGROUPID, raftServer, nil, nil)
	raftGroup.RegisterApplyHandle(rs.HandleCmd)
	raftGroup.RegisterPeerChangeHandle(rs.HandlePeerChange)
	raftGroup.RegisterGetSnapshotHandle(rs.HandleGetSnapshot)
	raftGroup.RegisterApplySnapshotHandle(rs.HandleApplySnapshot)
	raftGroup.RegisterLeaderChangeHandle(rs.LeaderChangeHandler)
	raftGroup.RegisterFatalEventHandle(rs.FatalEventHandler)

	raftPeers := make([]raftproto.Peer, 0, len(cfg.RaftNodes))
	for _, node := range cfg.RaftNodes {
		raftPeer := raftproto.Peer{
			Type:     raftproto.PeerNormal,
			ID:       node.NodeId,
			Priority: 0,
		}
		raftPeers = append(raftPeers, raftPeer)
	}
	raftConfig := &raft.RaftConfig{
		ID:           FIXED_RAFTGROUPID,
		Applied:      applyId,
		Peers:        raftPeers,
		Storage:      walStore,
		StateMachine: raftGroup,
	}

	rs.raftGroup = raftGroup
	rs.raftConfig = raftConfig
	rs.localStore = rowStore

	return nil
}

func (rs *RaftStore) raftLogCleanup() {
	defer rs.wg.Done()
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()
	for {
		select {
		case <-rs.ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						switch x := r.(type) {
						case string:
							fmt.Printf("Error: %s.\n", x)
						case error:
							fmt.Printf("Error: %s.\n", x.Error())
						default:
							fmt.Printf("Unknown panic error.%v", r)
						}
					}
				}()
				applyId := rs.localStore.Applied()
				if applyId < DEFAULT_RAFTLOG_LIMIT {
					return
				}
				rs.raftServer.Truncate(FIXED_RAFTGROUPID, applyId-DEFAULT_RAFTLOG_LIMIT)
			}()
		}
	}
}

type Resolver struct {
	nodes map[uint64]*Peer
}

func NewResolver(peers []*Peer) *Resolver {
	resolver := &Resolver{
		nodes: make(map[uint64]*Peer),
	}
	for _, p := range peers {
		resolver.nodes[p.NodeId] = p
	}
	return resolver
}

func (r *Resolver) NodeAddress(nodeID uint64, stype raft.SocketType) (addr string, err error) {
	switch stype {
	case raft.HeartBeat:
		node := r.nodes[nodeID]
		if node == nil {
			return "", ErrRaftInvalidNode
		}
		return node.RaftHeartbeatAddr, nil
	case raft.Replicate:
		node := r.nodes[nodeID]
		if node == nil {
			return "", ErrRaftInvalidNode
		}
		return node.RaftReplicateAddr, nil
	default:
		return "", ErrRaftUnknownType
	}
}

func (rs *RaftStore) becomeLeader(newLeaderId uint64) bool {
	return newLeaderId != 0 && rs.config.ClusterCfg.CurNodeId == newLeaderId
}
////////////////////raft end////////////////////////

/////////////////////callback implement begin///////////////
func (rs *RaftStore) raftKvRawGet(req *masterpb.RaftGetRequest, raftIndex uint64) (*masterpb.RaftGetResponse, error) {
	resp := new(masterpb.RaftGetResponse)
	//log.Info("raft put")
	// TODO write in one batch
	value, err := rs.localStore.Get(req.Key)
	if err != nil {
		//if err == sErr.ErrNotFound {
		//	resp.Code = ResponseCode_Success
		//	resp.Value = nil
		//	return resp, nil
		//}
		return nil, err
	}
	resp.Code = int32(masterpb.Success)
	resp.Value = value
	return resp, nil
}

func (rs *RaftStore) raftKvRawPut(req *masterpb.RaftPutRequest, raftIndex uint64) (*masterpb.RaftPutResponse, error) {
	resp := new(masterpb.RaftPutResponse)
	// TODO write in one batch
	err := rs.localStore.Put(req.Key, req.Value, raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = int32(masterpb.Success)
	return resp, nil
}

func (rs *RaftStore) raftKvRawDelete(req *masterpb.RaftDeleteRequest, raftIndex uint64) (*masterpb.RaftDeleteResponse, error) {
	resp := new(masterpb.RaftDeleteResponse)
	err := rs.localStore.Delete(req.Key, raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = int32(masterpb.Success)
	return resp, nil
}

func (rs *RaftStore) raftKvRawExecute(req *masterpb.ExecuteRequest, raftIndex uint64) (*masterpb.ExecuteResponse, error) {
	resp := new(masterpb.ExecuteResponse)
	batch := rs.localStore.NewWriteBatch()
	for _, e := range req.Execs {
		switch e.Do {
		case masterpb.ExecuteType_ExecPut:
			batch.Put(e.KvPair.Key, e.KvPair.Value, raftIndex)
		case masterpb.ExecuteType_ExecDelete:
			batch.Delete(e.KvPair.Key, raftIndex)
		}
	}
	err := batch.Commit()
	if err != nil {
		return nil, err
	}
	resp.Code = int32(masterpb.Success)
	return resp, nil
}

func (rs *RaftStore) HandleCmd(req *masterpb.Request, raftIndex uint64) (resp *masterpb.Response, err error) {
	resp = new(masterpb.Response)
	resp.CmdType = req.CmdType

	// TODO check split Status
	switch req.CmdType {
	case masterpb.CmdType_Get:
		_resp, err := rs.raftKvRawGet(req.GetReq, raftIndex)
		if err != nil {
			return nil, err
		}
		resp.GetResp = _resp
	case masterpb.CmdType_Put:
		_resp, err := rs.raftKvRawPut(req.PutReq, raftIndex)
		if err != nil {
			return nil, err
		}
		resp.PutResp = _resp
	case masterpb.CmdType_Delete:
		_resp, err := rs.raftKvRawDelete(req.DeleteReq, raftIndex)
		if err != nil {
			return nil, err
		}
		resp.DeleteResp = _resp
	case masterpb.CmdType_Execute:
		_resp, err := rs.raftKvRawExecute(req.ExecuteReq, raftIndex)
		if err != nil {
			return nil, err
		}
		resp.ExecuteResp = _resp
	}
	return resp, nil
}

func (rs *RaftStore) HandlePeerChange(confChange *raftproto.ConfChange) (res interface{}, err error) {
	switch confChange.Type {
	case raftproto.ConfAddNode:
		log.Debug("add raft node")
		res, err = nil, nil
	case raftproto.ConfRemoveNode:
		log.Debug("remove raft node")
		res, err = nil, nil
	case raftproto.ConfUpdateNode:
		log.Debug("update raft node")
		res, err = nil, nil
	default:
		res, err = nil, ErrUnknownRaftCmdType
	}

	return
}

////TODO
func (rs *RaftStore) HandleGetSnapshot() (raftkvstore.Snapshot, error) {
	log.Info("get snapshot")
	return rs.localStore.GetSnapshot()
}

func (rs *RaftStore) HandleApplySnapshot(peers []raftproto.Peer, iter *SnapshotKVIterator) error {
	log.Info("apply snapshot begin")

	var err error
	var pair *masterpb.RaftKvPair
	// TODO clear store and reopen store
	pathTemp := fmt.Sprintf("%s.%s", rs.raftStoreConfig.DataPath, time.Now().Format(time.RFC3339Nano))
	path := rs.raftStoreConfig.DataPath
	newStore, _, err := raftkvstore.NewBoltStore(dbBucket, raftBucket, pathTemp)
	if err != nil {
		log.Error("open new store failed, err[%v]", err)
		os.Remove(pathTemp)
		return err
	}
	for {
		pair, err = iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error("apply snapshot error [%v]", err)
			newStore.Close()
			os.Remove(pathTemp)
			return err
		}
		newStore.Put(pair.Key, pair.Value, pair.ApplyIndex)
	}
	log.Info("finish iterate snapshot")

	// 应用成功了
	newStore.Close()
	rs.localStore.Close()
	// 替换文件
	err = os.Rename(pathTemp, path)
	if err != nil {
		log.Fatal("rename db file failed, err %v", err)
	}
	newStore, _, err = raftkvstore.NewBoltStore(dbBucket, raftBucket, path)
	if err != nil {
		log.Fatal("reopen store failed, err[%v]", err)
	}
	rs.localStore = newStore
	log.Info("apply snapshot end")
	return nil
}

func (rs *RaftStore) LeaderChangeHandler(leaderId uint64) {
	log.Info("raft leader had changed to id[%v]", leaderId)

	if rs.leaderChangingCh == nil {
		return
	}

	var leaderNode *ClusterNode
	var leaderAddr string
	for _, node := range rs.config.ClusterCfg.Nodes {
		if node.NodeId == leaderId {
			leaderNode = node
			break
		}
	}
	if leaderNode != nil {
		leaderAddr = util.BuildAddr(leaderNode.Host, leaderNode.RpcPort)
    }

    info := &LeaderInfo{
        becomeLeader: rs.becomeLeader(leaderId),
        newLeaderId:  leaderId,
		newLeaderAddr: leaderAddr,
    }
    rs.leaderInfo = info
    select {
    case <-time.After(500 * time.Millisecond):
        log.Error("notify leader change timeout")
    case rs.leaderChangingCh <- info:
        log.Debug("notify leader change[%v] end", info)
    }
}

func (rs *RaftStore) FatalEventHandler(err *raft.FatalError) {
	log.Error("received raft fatal error[%v]", err)
}

///////////////////////callback implement end////////////////
