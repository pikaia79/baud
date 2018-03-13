package ps

import (
	"encoding/json"
	. "github.com/tiglabs//baud/schema"
	"github.com/tiglabs/baud/util/config"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type Container struct {
	cfg         *Cfg
	partitionID PartitionID
	isLeader    bool

	objectIndex *ObjectIndex
	edgeIndex   *EdgeIndex
}

func NewServer() *Container {
	return new(Conainer)
}

func (c *Container) Start(cfg *config.Config) error {
	return nil
}

func (c *Container) Shutdown() {}

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

func (s *Container) handleMutate(w http.ResponseWriter, r *http.Request) {}
