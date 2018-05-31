package zm

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/netutil"
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
		Name:         "master-api-server",
		Addr:         util.BuildAddr("0.0.0.0", config.ClusterCfg.CurNode.HttpPort),
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
	s.httpServer.Handle(netutil.GET, "/manage/db/list", s.handleDbList)
	s.httpServer.Handle(netutil.GET, "/manage/db/detail", s.handleDbDetail)

	s.httpServer.Handle(netutil.GET, "/manage/space/list", s.handleSpaceList)
	s.httpServer.Handle(netutil.GET, "/manage/space/detail", s.handleSpaceDetail)

	s.httpServer.Handle(netutil.GET, "/manage/partition/list", s.handlePartitionList)
	s.httpServer.Handle(netutil.GET, "/manage/ps/list", s.handlePSList)
}

func (s *ApiServer) handleDbList(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	dbs := s.cluster.DbCache.GetAllDBs()

	sendReply(w, newHttpSucReply(dbs))
}

func (s *ApiServer) handleDbDetail(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
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

func (s *ApiServer) handleSpaceDetail(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
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
	partitions := s.cluster.PartitionCache.GetAllPartitions()
	sendReply(w, newHttpSucReply(partitions))
}

func (s *ApiServer) handlePSList(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	allPs := s.cluster.PsCache.GetAllServers()
	sendReply(w, newHttpSucReply(allPs))
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
