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
	"proto/masterraftcmdpb"
	"sync"
	"time"
	"util/log"
	"util/raftkvstore"
)

const (
	FIXED_RAFTGROUPID          = 1
	DEFAULT_RAFTLOG_LIMIT      = 10000
	DEFAULT_SUBMIT_TIMEOUT_MAX = time.Second * 60
)

var (
	raftBucket []byte = []byte("MasterRaftBucket")
	dbBucket   []byte = []byte("MasterDbBucket")
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
	Close() error
}

type Batch interface {
	Put(key []byte, value []byte)
	Delete(key []byte)

	Commit() error
}

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
	return nil
}

func (rs *RaftStore) Put(key, value []byte) error {
	req := &masterraftcmdpb.Request{
		CmdType: masterraftcmdpb.CmdType_Put,
		PutReq: &masterraftcmdpb.PutRequest{
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
	req := &masterraftcmdpb.Request{
		CmdType: masterraftcmdpb.CmdType_Delete,
		DeleteReq: &masterraftcmdpb.DeleteRequest{
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
	req := &masterraftcmdpb.Request{
		CmdType: masterraftcmdpb.CmdType_Get,
		GetReq: &masterraftcmdpb.GetRequest{
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

	return lastErr
}

type SaveBatch struct {
	raft  *RaftGroup
	lock  sync.RWMutex
	batch []*masterraftcmdpb.KvPairExecute
}

func NewSaveBatch(raft *RaftGroup) *SaveBatch {
	return &SaveBatch{raft: raft, batch: nil}
}

func (b *SaveBatch) Put(key, value []byte) {
	_key := make([]byte, len(key))
	_value := make([]byte, len(value))
	copy(_key, key)
	copy(_value, value)
	exec := &masterraftcmdpb.KvPairExecute{
		Do:     masterraftcmdpb.ExecuteType_ExecPut,
		KvPair: &masterraftcmdpb.KvPair{Key: _key, Value: _value},
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	b.batch = append(b.batch, exec)
}

func (b *SaveBatch) Delete(key []byte) {
	_key := make([]byte, len(key))
	copy(_key, key)
	exec := &masterraftcmdpb.KvPairExecute{
		Do:     masterraftcmdpb.ExecuteType_ExecDelete,
		KvPair: &masterraftcmdpb.KvPair{Key: _key},
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
	req := &masterraftcmdpb.Request{
		CmdType: masterraftcmdpb.CmdType_Execute,
		ExecuteReq: &masterraftcmdpb.ExecuteRequest{
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

/////////////////////callback implement begin///////////////
func (rs *RaftStore) raftKvRawGet(req *masterraftcmdpb.GetRequest, raftIndex uint64) (*masterraftcmdpb.GetResponse, error) {
	resp := new(masterraftcmdpb.GetResponse)
	//log.Info("raft put")
	// TODO write in one batch
	value, err := rs.localStore.Get(req.GetKey())
	if err != nil {
		//if err == sErr.ErrNotFound {
		//	resp.Code = ResponseCode_Success
		//	resp.Value = nil
		//	return resp, nil
		//}
		return nil, err
	}
	resp.Code = int32(masterraftcmdpb.ResponseCode_Success)
	resp.Value = value
	return resp, nil
}

func (rs *RaftStore) raftKvRawPut(req *masterraftcmdpb.PutRequest, raftIndex uint64) (*masterraftcmdpb.PutResponse, error) {
	resp := new(masterraftcmdpb.PutResponse)
	// TODO write in one batch
	err := rs.localStore.Put(req.GetKey(), req.GetValue(), raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = int32(masterraftcmdpb.ResponseCode_Success)
	return resp, nil
}

func (rs *RaftStore) raftKvRawDelete(req *masterraftcmdpb.DeleteRequest, raftIndex uint64) (*masterraftcmdpb.DeleteResponse, error) {
	resp := new(masterraftcmdpb.DeleteResponse)
	err := rs.localStore.Delete(req.GetKey(), raftIndex)
	if err != nil {
		return nil, err
	}
	resp.Code = int32(masterraftcmdpb.ResponseCode_Success)
	return resp, nil
}

func (rs *RaftStore) raftKvRawExecute(req *masterraftcmdpb.ExecuteRequest, raftIndex uint64) (*masterraftcmdpb.ExecuteResponse, error) {
	resp := new(masterraftcmdpb.ExecuteResponse)
	batch := rs.localStore.NewWriteBatch()
	for _, e := range req.GetExecs() {
		switch e.Do {
		case masterraftcmdpb.ExecuteType_ExecPut:
			batch.Put(e.KvPair.Key, e.KvPair.Value, raftIndex)
		case masterraftcmdpb.ExecuteType_ExecDelete:
			batch.Delete(e.KvPair.Key, raftIndex)
		}
	}
	err := batch.Commit()
	if err != nil {
		return nil, err
	}
	resp.Code = int32(masterraftcmdpb.ResponseCode_Success)
	return resp, nil
}

func (rs *RaftStore) HandleCmd(req *masterraftcmdpb.Request, raftIndex uint64) (resp *masterraftcmdpb.Response, err error) {
	resp = new(masterraftcmdpb.Response)
	resp.CmdType = req.GetCmdType()

	// TODO check split Status
	switch req.GetCmdType() {
	case masterraftcmdpb.CmdType_Get:
		_resp, err := rs.raftKvRawGet(req.GetGetReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.GetResp = _resp
	case masterraftcmdpb.CmdType_Put:
		_resp, err := rs.raftKvRawPut(req.GetPutReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.PutResp = _resp
	case masterraftcmdpb.CmdType_Delete:
		_resp, err := rs.raftKvRawDelete(req.GetDeleteReq(), raftIndex)
		if err != nil {
			return nil, err
		}
		resp.DeleteResp = _resp
	case masterraftcmdpb.CmdType_Execute:
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
	var pair *masterraftcmdpb.RaftKvPair
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
