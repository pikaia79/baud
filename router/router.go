package router

import (
	"net/http"
	"strings"
	"httpd"
	"util/config"
)

type PSInfo struct {
	BeginSlot uint32
	EndSlot uint32
	psAddr string
}

type SpaceInfo struct {
	PSs []PSInfo
}

type DBInfo struct {
	Spaces map[string]SpaceInfo
}

type Router struct {
	httpServer *httpd.Service
	dbInfos map[string]DBInfo
}

func NewServer() *Router {
	return new(Router)
}

func (router *Router) Start(cfg *config.Config) error {
	router.httpServer = httpd.New(cfg.GetString("http"), router)
	return router.httpServer.Start()
}

func (router *Router) Shutdown() {
	router.httpServer.Close()
}

func (router *Router) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if err :=request.ParseForm(); err != nil {
		router.WriteHeader(writer, http.StatusBadRequest)
		return
	}
	switch {
	case strings.HasPrefix(request.URL.Path, "insert"):
		router.handleInsert(writer, request)
	case strings.HasPrefix(request.URL.Path, "get"):
		router.handleGet(writer, request)
	case strings.HasPrefix(request.URL.Path, "update"):
		router.handleUpdate(writer, request)
	case strings.HasPrefix(request.URL.Path, "delete"):
		router.handleDelete(writer, request)

	case strings.HasPrefix(request.URL.Path, "status"):
		router.handleStatus(writer, request)
	case strings.HasPrefix(request.URL.Path, "/debug/pprof"):
		router.handlePprof(writer, request)
	default:
		router.WriteHeader(writer, http.StatusNotFound)
	}
}

func (router *Router) handleInsert(writer http.ResponseWriter, request *http.Request) {
	dbName := request.FormValue("db")
	spaceName := request.FormValue("space")
}

func (router *Router) handleGet(writer http.ResponseWriter, request *http.Request) {
	dbName := request.FormValue("db")
	spaceName := request.FormValue("space")
	uid := request.FormValue("uid")
}

func (router *Router) handleUpdate(writer http.ResponseWriter, request *http.Request) {
	dbName := request.FormValue("db")
	spaceName := request.FormValue("space")
	uid := request.FormValue("uid")
}

func (router *Router) handleDelete(writer http.ResponseWriter, request *http.Request) {
	dbName := request.FormValue("db")
	spaceName := request.FormValue("space")
	uid := request.FormValue("uid")
}

func (router *Router) handleStatus(writer http.ResponseWriter, request *http.Request) {

}

func (router *Router) handlePprof(writer http.ResponseWriter, request *http.Request) {

}

func (router *Router) WriteHeader(writer http.ResponseWriter, status int) {
	writer.WriteHeader(status)
}

