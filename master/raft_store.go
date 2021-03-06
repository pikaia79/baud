package master

import (
	"errors"
	"fmt"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
	"golang.org/x/net/context"
	"io"
	"os"
	"path/filepath"
	"proto/ms_raftcmdpb"
	"sync"
	"time"
	"util/log"
	"util/raftkvstore"
)

const (
	FIXED_RAFTGROUPID          = 1
	DEFAULT_RAFTLOG_LIMIT      = 10000
	DEFAULT_SUBMIT_TIMEOUT_MAX = time.Second * 60
	SUCCESS                    = 0
)

var (
	raftBucket []byte = []byte("MasterRaftBucket")
	dbBucket   []byte = []byte("MasterDbBucket")

	ErrUnknownCommandType = errors.New("unknown command type")
)

//////////////////raft begin/////////////////
type RaftStore struct {
	config     *Config
	localStore raftkvstore.Store
	localRead  bool

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
	//WebManageAddr     string
	//RpcServerAddr     string
	RaftHeartbeatAddr string
	RaftReplicateAddr string
}

type RaftStoreConfig struct {
	RaftRetainLogs        uint64
	RaftHeartbeatInterval time.Duration
	RaftHeartbeatAddr     string
	RaftReplicateAddr     string
	RaftNodes             []*Peer
	//RaftLeaderChangeNotifier	[]chan*Peer

	NodeId   uint64
	DataPath string
	WalPath  string

	//LeaderChangeHandler raftgroup.RaftLeaderChangeHandler
	//FatalHandler        raftgroup.RaftFatalEventHandler
}

func NewRaftStore(config *Config) *RaftStore {
	rs := &RaftStore{
		config:    config,
		localRead: true,
	}
	rs.ctx, rs.ctxCancel = context.WithCancel(context.Background())
	return rs
}

func (rs *RaftStore) Start() error {
	if err := rs.initRaftStoreCfg(); err != nil {
		return err
	}
	if err := rs.initRaftServer(); err != nil {
		return err
	}
	return nil
}

func (rs *RaftStore) Close() {
	if rs.raftGroup != nil {
		if err := rs.raftGroup.Release(); err != nil {
			log.Error("fail to close raftgroup. err:[%v]", err)
		}
	}

	if rs.ctxCancel != nil {
		rs.ctxCancel()
	}
	rs.wg.Wait()

	if rs.localStore != nil {
		if err := rs.localStore.Close(); err != nil {
			log.Error("fail to close boltdb store. err:[%v]", err)
		}
	}
}

func (rs *RaftStore) initRaftStoreCfg() error {
	raftStoreCfg := new(RaftStoreConfig)
	raftStoreCfg.NodeId = rs.config.NodeId

	dataRootDir := filepath.Join(rs.config.DataPath, "data")
	if err := os.MkdirAll(dataRootDir, 0755); err != nil {
		log.Error("make data root direcotory %rs failed, err[%v]", dataRootDir, err)
		return err
	}
	raftStoreCfg.DataPath = filepath.Join(dataRootDir, "baud.db")
	raftStoreCfg.WalPath = filepath.Join(dataRootDir, ".wal")

	raftStoreCfg.RaftRetainLogs = rs.config.Raft.RetainLogsCount
	raftStoreCfg.RaftHeartbeatInterval = rs.config.Raft.HeartbeatInterval.Duration
	raftStoreCfg.RaftHeartbeatAddr = rs.config.raftHeartbeatAddr
	raftStoreCfg.RaftReplicateAddr = rs.config.raftReplicaAddr

	var peers []*Peer
	for _, p := range rs.config.Cluster.Peers {
		peer := new(Peer)
		peer.NodeId = p.ID
		//node.WebManageAddr = fmt.Sprintf("%rs:%d", peer.Host, peer.HttpPort)
		//node.RpcServerAddr = fmt.Sprintf("%rs:%d", peer.Host, peer.RpcPort)
		peer.RaftHeartbeatAddr = fmt.Sprintf("%s:%d", p.Host, p.RaftPorts[0])
		peer.RaftReplicateAddr = fmt.Sprintf("%s:%d", p.Host, p.RaftPorts[1])
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
	//raftGroup.RegisterLeaderChangeHandle(conf.LeaderChangeHandler)
	//raftGroup.RegisterFatalEventHandle(conf.FatalHandler)

	raftPeers := make([]raftproto.Peer, len(cfg.RaftNodes))
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

//func (rs *RaftStore) registerLeaderChangeNotifier(notifier chan*Peer) {
//	rs.raftStoreConfig.RaftLeaderChangeNotifier = append(rs.raftStoreConfig.RaftLeaderChangeNotifier, notifier)
//}

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
	resolver := new(Resolver)
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
			return "", errors.New("invalid raft node")
		}
		return node.RaftHeartbeatAddr, nil
	case raft.Replicate:
		node := r.nodes[nodeID]
		if node == nil {
			return "", errors.New("invalid raft node")
		}
		return node.RaftReplicateAddr, nil
	default:
		return "", errors.New("unknown raft socket type")
	}
}

////////////////////raft end////////////////////////

////////////////////store begin///////////////////
type Store interface {
	Open() error
	Put(key, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Scan(startKey, limitKey []byte) raftkvstore.Iterator
	NewBatch() Batch
	Close() error
}

type Batch interface {
	Put(key []byte, value []byte)
	Delete(key []byte)

	Commit() error
}

func (rs *RaftStore) Open() error {
	if err := rs.raftGroup.Start(rs.raftConfig); err != nil {
		return err
	}
	rs.wg.Add(1)
	go rs.raftLogCleanup()
	return nil
}

func (rs *RaftStore) Put(key, value []byte) error {
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Put,
		PutReq: &ms_raftcmdpb.PutRequest{
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
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Delete,
		DeleteReq: &ms_raftcmdpb.DeleteRequest{
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
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Get,
		GetReq: &ms_raftcmdpb.GetRequest{
			Key: key,
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), DEFAULT_SUBMIT_TIMEOUT_MAX)
	defer cancel()
	resp, err := rs.raftGroup.SubmitCommand(ctx, req)
	if err != nil {
		return nil, err
	}
	value := resp.GetGetResp().GetValue()
	return value, nil
}

func (rs *RaftStore) Scan(startKey, limitKey []byte) raftkvstore.Iterator {
	// TODO: when localRead is false
	return rs.localStore.NewIterator(startKey, limitKey)
}

func (rs *RaftStore) NewBatch() Batch {
	return NewSaveBatch(rs.raftGroup)
}

type SaveBatch struct {
	raft  *RaftGroup
	lock  sync.RWMutex
	batch []*ms_raftcmdpb.KvPairExecute
}

func NewSaveBatch(raft *RaftGroup) *SaveBatch {
	return &SaveBatch{raft: raft, batch: nil}
}

func (b *SaveBatch) Put(key, value []byte) {
	_key := make([]byte, len(key))
	_value := make([]byte, len(value))
	copy(_key, key)
	copy(_value, value)
	exec := &ms_raftcmdpb.KvPairExecute{
		Do:     ms_raftcmdpb.ExecuteType_ExecPut,
		KvPair: &ms_raftcmdpb.KvPair{Key: _key, Value: _value},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Delete(key []byte) {
	_key := make([]byte, len(key))
	copy(_key, key)
	exec := &ms_raftcmdpb.KvPairExecute{
		Do:     ms_raftcmdpb.ExecuteType_ExecDelete,
		KvPair: &ms_raftcmdpb.KvPair{Key: _key},
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
	req := &ms_raftcmdpb.Request{
		CmdType: ms_raftcmdpb.CmdType_Execute,
		ExecuteReq: &ms_raftcmdpb.ExecuteRequest{
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

/////////////////////callback implement begin///////////////
func (rs *RaftStore) raftKvRawGet(req *ms_raftcmdpb.GetRequest, raftIndex uint64) (*ms_raftcmdpb.GetResponse, error) {
	resp := new(ms_raftcmdpb.GetResponse)
	//log.Info("raft put")
	// TODO write in one batch
	value, err := rs.localStore.Get(req.GetKey())
	if err != nil {
		//if err == sErr.ErrNotFound {
		//	resp.Code = SUCCESS
		//	resp.Value = nil
		//	return resp, nil
		//}
		return nil, err
	}
	resp.Code = SUCCESS
	resp.Value = value
	return resp, nil
}

func (rs *RaftStore) raftKvRawPut(req *ms_raftcmdpb.PutRequest, raftIndex uint64) (*ms_raftcmdpb.PutResponse, error) {
	resp := new(ms_raftcmdpb.PutResponse)
	// TODO write in one batch
	err := rs.localStore.Put(req.GetKey(), req.GetValue(), raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (rs *RaftStore) raftKvRawDelete(req *ms_raftcmdpb.DeleteRequest, raftIndex uint64) (*ms_raftcmdpb.DeleteResponse, error) {
	resp := new(ms_raftcmdpb.DeleteResponse)
	err := rs.localStore.Delete(req.GetKey(), raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (rs *RaftStore) raftKvRawExecute(req *ms_raftcmdpb.ExecuteRequest, raftIndex uint64) (*ms_raftcmdpb.ExecuteResponse, error) {
	resp := new(ms_raftcmdpb.ExecuteResponse)
	batch := rs.localStore.NewWriteBatch()
	for _, e := range req.GetExecs() {
		switch e.Do {
		case ms_raftcmdpb.ExecuteType_ExecPut:
			batch.Put(e.KvPair.Key, e.KvPair.Value, raftIndex)
		case ms_raftcmdpb.ExecuteType_ExecDelete:
			batch.Delete(e.KvPair.Key, raftIndex)
		}
	}
	err := batch.Commit()
	if err != nil {
		return nil, err
	}
	resp.Code = SUCCESS
	return resp, nil
}

func (rs *RaftStore) HandleCmd(req *ms_raftcmdpb.Request, raftIndex uint64) (resp *ms_raftcmdpb.Response, err error) {
	resp = new(ms_raftcmdpb.Response)
	resp.CmdType = req.GetCmdType()

	// TODO check split status
	switch req.GetCmdType() {
	case ms_raftcmdpb.CmdType_Get:
		_resp, err := rs.raftKvRawGet(req.GetGetReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.GetResp = _resp
	case ms_raftcmdpb.CmdType_Put:
		_resp, err := rs.raftKvRawPut(req.GetPutReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.PutResp = _resp
	case ms_raftcmdpb.CmdType_Delete:
		_resp, err := rs.raftKvRawDelete(req.GetDeleteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.DeleteResp = _resp
	case ms_raftcmdpb.CmdType_Execute:
		_resp, err := rs.raftKvRawExecute(req.GetExecuteReq(), raftIndex)
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

		res, err = nil, nil
	case raftproto.ConfRemoveNode:

		res, err = nil, nil
	case raftproto.ConfUpdateNode:
		log.Debug("update range peer")
		res, err = nil, nil
	default:
		res, err = nil, ErrUnknownCommandType
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
	var pair *ms_raftcmdpb.RaftKvPair
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

///////////////////////callback implement end////////////////
