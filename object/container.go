package object

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type Container struct{}

func (s *Container) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "join"):
		s.handleJoin(w, r)
	case strings.HasPrefix(r.URL.Path, "remove"):
		s.handleRemove(w, r)
	case strings.HasPrefix(r.URL.Path, "backup"):
		s.handleBackup(w, r)
	case strings.HasPrefix(r.URL.Path, "insert"):
		s.handleInsert(w, r)
	case strings.HasPrefix(r.URL.Path, "update"):
		s.handleUpdate(w, r)
	case strings.HasPrefix(r.URL.Path, "delete"):
		s.handleDelete(w, r)
	case strings.HasPrefix(r.URL.Path, "query"):
		s.handleQuery(w, r)
	case strings.HasPrefix(r.URL.Path, "status"):
		s.handleStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof"):
		s.handlePprof(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Container) handleQuery(w http.ResponseWriter, r *http.Request) {}
