package master

import (
	"util/server"
	"net/http"
)

const (
	DEFAULT_CONN_LIMIT		= 10000
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

