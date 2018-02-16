package baudserver

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type Service struct {
	addr string
	ln   net.Listener

	start time.Time

	logger *log.Logger

	p *Partition
}

func New(addr string) *Service {
	return &Service{
		addr: addr,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s,
	}

	var err error

	s.ln, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}

	go func() {
		err := server.Serve(s.ln)
		if err != nil {
			s.logger.Println("HTTP service returned:", err.Error())
		}
	}()

	s.logger.Println("service listening on", s.addr)

	return nil
}

func (s *Service) Close() {
	s.ln.Close()
	return
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
func (s *Service) handleExecute(w http.ResponseWriter, r *http.Request) {}
