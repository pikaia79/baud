package master

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"util/log"
	"util/server"
)

const (
	DEFAULT_CONN_LIMIT = 10000

	// definition for http url parameter name
	DB_NAME         = "db_name"
	SRC_DB_NAME     = "src_db_name"
	DEST_DB_NAME    = "dest_db_name"
	SPACE_NAME      = "space_name"
	SPACE_ID		= "space_id"
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
		Sock:      config.webManageAddr,
		Version:   "v1",
		ConnLimit: DEFAULT_CONN_LIMIT,
	})

	return apiServer
}

func (s *ApiServer) Start() error {
	return s.httpServer.Run()
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
	s.httpServer.Handle("/manage/space/create", s.handleSpaceCreate)
	s.httpServer.Handle("/manage/space/delete", s.handleSpaceDelete)
	s.httpServer.Handle("/manage/space/rename", s.handleSpaceRename)
	s.httpServer.Handle("/manage/space/detail", s.handleSpaceDetail)
	s.httpServer.Handle("/manage/index/create", s.handleIndexCreate)
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
	partitionNum, err := checkMissingAndNumericParam(w, r, PARTITION_NUM)
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
	spaceId, err := checkMissingAndNumericParam(w, r, SPACE_ID)
	if err != nil {
		return
	}

	s.cluster.detailSpace(spaceId)
}

func (s *ApiServer) handleIndexCreate(w http.ResponseWriter, r *http.Request) {
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

	code, ok := httpErrMap[err.Error()]
	if ok {
		return &HttpReply{
			Code: code,
			Msg:  err.Error(),
		}
	} else {
		return &HttpReply{
			Code:		ERRCODE_INTERNAL_ERROR,
			Msg:		ErrInternalError.Error(),
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
		return nil, ErrParamError
	}
	return paramVal, nil
}

func checkMissingAndNumericParam(w http.ResponseWriter, r *http.Request, paramName string) (int, error) {
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
		return nil, ErrParamError
	}
	return paramValInt, nil
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
