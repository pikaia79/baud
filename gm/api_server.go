package gm

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/netutil"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	DEFAULT_CONN_LIMIT    = 100
	DEFAULT_CLOSE_TIMEOUT = 5 * time.Second

	// definition for http url parameter name
	DB_NAME         = "db_name"
	SRC_DB_NAME     = "src_db_name"
	DEST_DB_NAME    = "dest_db_name"
	SPACE_NAME      = "space_name"
	SRC_SPACE_NAME  = "src_space_name"
	DEST_SPACE_NAME = "dest_space_name"
	PARTITION_KEY   = "partition_key"
	PARTITION_FUNC  = "partition_func"
	PARTITION_NUM   = "partition_num"
)

type ApiServer struct {
	config     *Config
	httpServer *netutil.Server
	cluster    *Cluster
	wg         sync.WaitGroup
}

func NewApiServer(config *Config, cluster *Cluster) *ApiServer {
	cfg := &netutil.ServerConfig{
		Name:         "gm-api-server",
		Addr:         util.BuildAddr("0.0.0.0", config.ClusterCfg.HttpPort),
		Version:      "v1",
		ConnLimit:    DEFAULT_CONN_LIMIT,
		CloseTimeout: DEFAULT_CLOSE_TIMEOUT,
	}

	apiServer := &ApiServer{
		config:     config,
		httpServer: netutil.NewServer(cfg),
		cluster:    cluster,
	}
	apiServer.initAdminHandler()

	return apiServer
}

func (s *ApiServer) Start() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		if err := s.httpServer.Run(); err != nil {
			log.Error("api server run error[%v]", err)
		}
	}()

	log.Info("ApiServer has started")
	return nil
}

func (s *ApiServer) Close() {
	if s.httpServer != nil {
		s.httpServer.Shutdown()
		s.httpServer = nil
	}

	s.wg.Wait()

	log.Info("ApiServer has closed")
}

func (s *ApiServer) initAdminHandler() {
	s.httpServer.Handle(netutil.POST, "/manage/db/create", s.handleDbCreate)
	s.httpServer.Handle(netutil.PUT, "/manage/db/delete", s.handleDbDelete)
	s.httpServer.Handle(netutil.DELETE, "/manage/db/rename", s.handleDbRename)
	s.httpServer.Handle(netutil.GET, "/manage/db/list", s.handleDbList)
	s.httpServer.Handle(netutil.GET, "/manage/db/detail", s.handleDbDetail)

	s.httpServer.Handle(netutil.POST, "/manage/space/create", s.handleSpaceCreate)
	s.httpServer.Handle(netutil.PUT, "/manage/space/delete", s.handleSpaceDelete)
	s.httpServer.Handle(netutil.DELETE, "/manage/space/rename", s.handleSpaceRename)
	s.httpServer.Handle(netutil.GET, "/manage/space/list", s.handleSpaceList)
	s.httpServer.Handle(netutil.GET, "/manage/space/detail", s.handleSpaceDetail)

	s.httpServer.Handle(netutil.GET, "/manage/partition/list", s.handlePartitionList)
}

func (s *ApiServer) handleDbCreate(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}

	db, err := s.cluster.CreateDb(dbName)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(db))
}

func (s *ApiServer) handleDbDelete(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}
}

func (s *ApiServer) handleDbRename(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	srcDbName, err := checkMissingParam(w, r, SRC_DB_NAME)
	if err != nil {
		return
	}
	destDbName, err := checkMissingParam(w, r, DEST_DB_NAME)
	if err != nil {
		return
	}

	if err := s.cluster.RenameDb(srcDbName, destDbName); err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(""))
}

func (s *ApiServer) handleDbList(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	dbs := s.cluster.DbCache.GetAllDBs()

	sendReply(w, newHttpSucReply(dbs))
}

func (s *ApiServer) handleDbDetail(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}

	db := s.cluster.DbCache.FindDbByName(dbName)
	if db == nil {
		sendReply(w, newHttpErrReply(ErrDbNotExists))
		return
	}

	sendReply(w, newHttpSucReply(db))
}

func (s *ApiServer) handleSpaceCreate(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}
	spaceName, err := checkMissingParam(w, r, SPACE_NAME)
	if err != nil {
		return
	}
	partitionKey, err := checkMissingParam(w, r, PARTITION_KEY)
	if err != nil {
		return
	}

	partitionFunc, err := checkMissingParam(w, r, PARTITION_FUNC)
	if err != nil {
		return
	}
	partitionNum, err := checkMissingAndUint64Param(w, r, PARTITION_NUM)
	if err != nil {
		return
	}

	policy := &PartitionPolicy{
		Key:      partitionKey,
		Function: partitionFunc,
		Number:   partitionNum,
	}
	space, err := s.cluster.CreateSpace(dbName, spaceName, policy)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(space))
}

func (s *ApiServer) handleSpaceDelete(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}
}

func (s *ApiServer) handleSpaceRename(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}
	srcSpaceName, err := checkMissingParam(w, r, SRC_SPACE_NAME)
	if err != nil {
		return
	}
	destSpaceName, err := checkMissingParam(w, r, DEST_SPACE_NAME)
	if err != nil {
		return
	}

	if err := s.cluster.RenameSpace(dbName, srcSpaceName, destSpaceName); err != nil {
		sendReply(w, newHttpErrReply(err))
	}

	sendReply(w, newHttpSucReply(""))
}

func (s *ApiServer) handleSpaceDetail(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}
	spaceName, err := checkMissingParam(w, r, SPACE_NAME)
	if err != nil {
		return
	}

	db := s.cluster.DbCache.FindDbByName(dbName)
	if db == nil {
		sendReply(w, newHttpErrReply(ErrDbNotExists))
		return
	}

	space := db.SpaceCache.FindSpaceByName(spaceName)
	if space == nil {
		sendReply(w, newHttpErrReply(ErrSpaceNotExists))
		return
	}

	sendReply(w, newHttpSucReply(space))
}

func (s *ApiServer) handleSpaceList(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}

	db := s.cluster.DbCache.FindDbByName(dbName)
	if db == nil {
		sendReply(w, newHttpErrReply(ErrDbNotExists))
		return
	}

	sendReply(w, newHttpSucReply(db.SpaceCache.GetAllSpaces()))
}

func (s *ApiServer) handlePartitionList(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	if err := s.checkLeader(w); err != nil {
		return
	}

	partitions := s.cluster.PartitionCache.GetAllPartitions()
	sendReply(w, newHttpSucReply(partitions))
}

type HttpReply struct {
	Code int32       `json:"code"`
	Msg  string      `json:"msg"`
	Data interface{} `json:"data"`
}

func newHttpSucReply(data interface{}) *HttpReply {
	return &HttpReply{
		Code: ERRCODE_SUCCESS,
		Msg:  ErrSuc.Error(),
		Data: data,
	}
}

func newHttpErrReply(err error) *HttpReply {
	if err == nil {
		return newHttpSucReply("")
	}

	code, ok := Err2CodeMap[err]
	if ok {
		return &HttpReply{
			Code: code,
			Msg:  err.Error(),
		}
	} else {
		return &HttpReply{
			Code: ERRCODE_INTERNAL_ERROR,
			Msg:  ErrInternalError.Error(),
		}
	}
}

func (s *ApiServer) checkLeader(w http.ResponseWriter) error {
	leaderInfo := s.cluster.store.GetLeaderSync()

	if leaderInfo == nil {
		sendReply(w, newHttpErrReply(ErrNoMSLeader))
		return ErrNoMSLeader
	}

	if !leaderInfo.becomeLeader {
		if leaderInfo.newLeaderId == 0 {
			sendReply(w, newHttpErrReply(ErrNoMSLeader))
			return ErrNoMSLeader
		} else {
			log.Debug("current master leader is [%v]", leaderInfo.newLeaderId)
			reply := newHttpErrReply(ErrNotMSLeader)
			newMsg := fmt.Sprintf("%s, current leader[%d][%s]", reply.Msg, leaderInfo.newLeaderId,
				leaderInfo.newLeaderAddr)
			reply.Msg = newMsg
			sendReply(w, reply)
			return ErrNotMSLeader
		}
	}

	return nil
}

func checkMissingParam(w http.ResponseWriter, r *http.Request, paramName string) (string, error) {
	paramVal := r.FormValue(paramName)
	if paramVal == "" {
		reply := newHttpErrReply(ErrParamError)
		newMsg := fmt.Sprintf("%s. missing[%s]", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return "", ErrParamError
	}
	return paramVal, nil
}

func checkMissingAndUint32Param(w http.ResponseWriter, r *http.Request, paramName string) (uint32, error) {
	paramValStr, err := checkMissingParam(w, r, paramName)
	if err != nil {
		return 0, err
	}

	paramValInt, err := strconv.Atoi(paramValStr)
	if err != nil {
		reply := newHttpErrReply(ErrParamError)
		newMsg := fmt.Sprintf("%s, unmatched type[%s]", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return 0, ErrParamError
	}
	if paramValInt > math.MaxUint32 {
		reply := newHttpErrReply(ErrParamError)
		newMsg := fmt.Sprintf("%s, value of [%s] exceed uint32 limit", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return 0, ErrParamError
	}
	return uint32(paramValInt), nil
}

func checkMissingAndUint64Param(w http.ResponseWriter, r *http.Request, paramName string) (uint64, error) {
	paramValStr, err := checkMissingParam(w, r, paramName)
	if err != nil {
		return 0, err
	}

	paramValInt, err := strconv.Atoi(paramValStr)
	if err != nil {
		reply := newHttpErrReply(ErrParamError)
		newMsg := fmt.Sprintf("%s, unmatched type[%s]", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return 0, ErrParamError
	}
	if paramValInt > math.MaxUint64 {
		reply := newHttpErrReply(ErrParamError)
		newMsg := fmt.Sprintf("%s, value of [%s] exceed uint32 limit", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return 0, ErrParamError
	}
	return uint64(paramValInt), nil
}

func sendReply(w http.ResponseWriter, httpReply *HttpReply) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.Error("fail to marshal http reply[%v]. err:[%v]", httpReply, err)
		sendReply(w, newHttpErrReply(ErrInternalError))
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("fail to write http reply[%s] len[%d]. err:[%v]", string(reply), len(reply), err)
	}
}
