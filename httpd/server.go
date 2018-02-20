package httpd

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

	handler http.Handler
	start   time.Time

	logger *log.Logger
}

func New(addr string, handler http.Handler) *Service {
	return &Service{
		addr:    addr,
		handler: handler,
	}
}

func (s *Service) Start() error {
	server := http.Server{
		Handler: s.handler,
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
