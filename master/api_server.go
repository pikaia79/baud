package master

import (
	"util/server"
	"net/http"
	"strconv"
	"encoding/json"
	"util/log"
	"fmt"
)

const (
	DEFAULT_CONN_LIMIT		= 10000

	// definition for http url parameter name
	DB_NAME		= "db_name"
)

type ApiServer struct {
	config 			*Config
	httpServer 		*server.Server
	cluster 		*Cluster
}

func NewApiServer(config *Config, cluster *Cluster) *ApiServer {
	apiServer := &ApiServer{
		config:		config,
		httpServer:	server.NewServer(),
		cluster:	cluster,
	}

	apiServer.httpServer.Init("master-api-server", &server.ServerConfig{
		Sock:		config.webManageAddr,
		Version:	"v1",
		ConnLimit: 	DEFAULT_CONN_LIMIT,
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
}

func (s *ApiServer) handleSpaceCreate(w http.ResponseWriter, r *http.Request) {
}

func (s *ApiServer) handleSpaceDelete(w http.ResponseWriter, r *http.Request) {
}

func (s *ApiServer) handleSpaceRename(w http.ResponseWriter, r *http.Request) {
}

func (s *ApiServer) handleIndexCreate(w http.ResponseWriter, r *http.Request) {
}


type HttpReply struct {
	Code    int32       `json:"code"`
	Msg 	string      `json:"msg"`
	Data    interface{} `json:"data"`
}

func newHttpSucReply(data interface{}) *HttpReply {
	return &HttpReply{
		Code:	ERRCODE_SUCCESS,
		Msg:	ErrSuc.Error(),
		Data:	data,
	}
}

func newHttpErrReply(err error) *HttpReply {
	if err == nil {
		return newHttpSucReply("")
	}

	code, ok := httpErrMap[err.Error()]
	if ok {
		return &HttpReply{
			Code:		code,
			Msg:		err.Error(),
		}
	} else {
		return &HttpReply{
			Code:		ERRCODE_INTERNAL_ERROR,
			Msg:		ErrIntarnalError.Error(),
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

func sendReply(w http.ResponseWriter, httpReply *HttpReply) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.Error("fail to marshal http reply[%v]. err:[%v]", httpReply, err)
		sendReply(w, newHttpErrReply(ErrIntarnalError))
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("fail to write http reply[%s] len[%d]. err:[%v]", string(reply), len(reply), err)
	}
}
