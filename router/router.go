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

func (r *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "execute"):
		r.handleExecute(w, r)
	case strings.HasPrefix(r.URL.Path, "query"):
		r.handleQuery(w, r)
	case strings.HasPrefix(r.URL.Path, "status"):
		r.handleStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof"):
		r.handlePprof(w, r)
	default:
		r.WriteHeader(http.StatusNotFound)
	}
}
