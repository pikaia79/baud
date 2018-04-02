package router

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type Router struct{}

func NewServer() *Router {
	return new(Router)
}

func (r *Router) Start(cfg *config.Config) error {
	return nil
}

func (r *Router) Shutdown() {}

func (r *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "insert"):
		r.handleInsert(w, r)
	case strings.HasPrefix(r.URL.Path, "get"):
		r.handleGet(w, r)
	case strings.HasPrefix(r.URL.Path, "update"):
		r.handleUpdate(w, r)
	case strings.HasPrefix(r.URL.Path, "delete"):
		r.handleDelete(w, r)

	case strings.HasPrefix(r.URL.Path, "status"):
		r.handleStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof"):
		r.handlePprof(w, r)
	default:
		r.WriteHeader(http.StatusNotFound)
	}
}
