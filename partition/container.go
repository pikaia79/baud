package partition

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

func (s *Container) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "join"):
		s.handleJoin(w, r)
	case strings.HasPrefix(r.URL.Path, "remove"):
		s.handleRemove(w, r)
	case strings.HasPrefix(r.URL.Path, "backup"):
		s.handleBackup(w, r)
	case strings.HasPrefix(r.URL.Path, "execute"):
		s.handleExecute(w, r)
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

//create a partition or insert/update objects
func (s *Container) handleExecute(w http.ResponseWriter, r *http.Request) {}
