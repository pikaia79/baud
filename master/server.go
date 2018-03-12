package master

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type Master struct{}

func NewServer() *Master {
	return new(Master)
}

func (s *Master) Start(cfg *config.Config) error {
	return nil
}

func (s *Master) Shutdown() {}

func (s *Master) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "createdatabase"):
		s.handleJoin(w, r)
	case strings.HasPrefix(r.URL.Path, "createspace"):
		s.handleRemove(w, r)
	case strings.HasPrefix(r.URL.Path, "status"):
		s.handleStatus(w, r)
	case strings.HasPrefix(r.URL.Path, "/debug/pprof"):
		s.handlePprof(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}
