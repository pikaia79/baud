package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/server"
	"math"
	"net/http"
	"strconv"
)

const (
	DEFAULT_CONN_LIMIT = 100

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
	httpServer *server.Server
	cluster    *Cluster
}

func NewApiServer(config *Config, cluster *Cluster) *ApiServer {
	apiServer := &ApiServer{
		config:     config,
		httpServer: server.NewServer(),
		cluster:    cluster,
	}

	apiServer.httpServer.Init("master-api-server", &server.ServerConfig{
		Sock:      util.BuildAddr("0.0.0.0", int(config.ClusterCfg.CurNode.HttpPort)),
		Version:   "v1",
		ConnLimit: DEFAULT_CONN_LIMIT,
	})

	apiServer.initAdminHandler()

	return apiServer
}

func (s *ApiServer) Start() error {
	go func() {
		if err := s.httpServer.Run(); err != nil {
			log.Error("api server run error[%v]", err)
		}
	}()
	return nil
}

func (s *ApiServer) Close() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
}

func (s *ApiServer) initAdminHandler() {
	s.httpServer.Handle("/manage/db/create", s.handleDbCreate)
	s.httpServer.Handle("/manage/db/delete", s.handleDbDelete)
	s.httpServer.Handle("/manage/db/rename", s.handleDbRename)
	s.httpServer.Handle("/manage/db/list", s.handleDbList)
	s.httpServer.Handle("/manage/db/detail", s.handleDbDetail)
	s.httpServer.Handle("/manage/space/create", s.handleSpaceCreate)
	s.httpServer.Handle("/manage/space/delete", s.handleSpaceDelete)
	s.httpServer.Handle("/manage/space/rename", s.handleSpaceRename)
	s.httpServer.Handle("/manage/space/list", s.handleSpaceList)
	s.httpServer.Handle("/manage/space/detail", s.handleSpaceDetail)
}

func (s *ApiServer) handleDbCreate(w http.ResponseWriter, r *http.Request) {
	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}

	db, err := s.cluster.createDb(dbName)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(db))
}

func (s *ApiServer) handleDbDelete(w http.ResponseWriter, r *http.Request) {
}

func (s *ApiServer) handleDbRename(w http.ResponseWriter, r *http.Request) {
	srcDbName, err := checkMissingParam(w, r, SRC_DB_NAME)
	if err != nil {
		return
	}
	destDbName, err := checkMissingParam(w, r, DEST_DB_NAME)
	if err != nil {
		return
	}

	if err := s.cluster.renameDb(srcDbName, destDbName); err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(""))
}

func (s *ApiServer) handleDbList(w http.ResponseWriter, r *http.Request) {
	dbs := s.cluster.dbCache.getAllDBs()

	sendReply(w, newHttpSucReply(dbs))
}

func (s *ApiServer) handleDbDetail(w http.ResponseWriter, r *http.Request) {
	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}

	db := s.cluster.dbCache.findDbByName(dbName)
	if db == nil {
		sendReply(w, newHttpErrReply(ErrDbNotExists))
		return
	}

	sendReply(w, newHttpSucReply(db))
}

func (s *ApiServer) handleSpaceCreate(w http.ResponseWriter, r *http.Request) {
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
	partitionNum, err := checkMissingAndUint32Param(w, r, PARTITION_NUM)
	if err != nil {
		return
	}

	space, err := s.cluster.createSpace(dbName, spaceName, partitionKey, partitionFunc, partitionNum)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(space))
}

func (s *ApiServer) handleSpaceDelete(w http.ResponseWriter, r *http.Request) {
}

func (s *ApiServer) handleSpaceRename(w http.ResponseWriter, r *http.Request) {
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

	if err := s.cluster.renameSpace(dbName, srcSpaceName, destSpaceName); err != nil {
		sendReply(w, newHttpErrReply(err))
	}

	sendReply(w, newHttpSucReply(""))
}

func (s *ApiServer) handleSpaceDetail(w http.ResponseWriter, r *http.Request) {
	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}
	spaceName, err := checkMissingParam(w, r, SPACE_NAME)
	if err != nil {
		return
	}

	db := s.cluster.dbCache.findDbByName(dbName)
	if db == nil {
		sendReply(w, newHttpErrReply(ErrDbNotExists))
		return
	}

	space := db.spaceCache.findSpaceByName(spaceName)
	if space == nil {
		sendReply(w, newHttpErrReply(ErrSpaceNotExists))
		return
	}

	sendReply(w, newHttpSucReply(space))
}

func (s *ApiServer) handleSpaceList(w http.ResponseWriter, r *http.Request) {
	dbName, err := checkMissingParam(w, r, DB_NAME)
	if err != nil {
		return
	}

	db := s.cluster.dbCache.findDbByName(dbName)
	if db == nil {
		sendReply(w, newHttpErrReply(ErrDbNotExists))
		return
	}

	sendReply(w, newHttpSucReply(db.spaceCache.getAllSpaces()))
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
