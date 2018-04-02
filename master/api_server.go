package master

import (
	"util/server"
)

const (
	DEFAULT_CONN_LIMIT		= 10000
)

type ApiServer struct {
	config 			*Config
	httpServer 		*server.Server
}

func NewApiServer(config *Config) *ApiServer {
	apiServer := &ApiServer{
		config:		config,
		httpServer:	server.NewServer(),
	}

	apiServer.httpServer.Init("master-api-server", &server.ServerConfig{
		Sock:		config.webManageAddr,
		Version:	"v1",
		ConnLimit: 	DEFAULT_CONN_LIMIT,
	})

	return apiServer
}

func (s *ApiServer) Start() {
	s.httpServer.Run()
}

func (s *ApiServer) Close() {
	if s.httpServer != nil {
		s.httpServer.Close()
	}
}


func (s *ApiServer) initAdminHandler() {

}