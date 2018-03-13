package master

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

type Master struct {
	cfg *Cfg

	databases map[string]*DBInfo

	zones map[string]*ZoneInfo
}

func NewServer() *Master {
	return new(Master)
}

func (s *Master) Start(cfg *config.Config) error {
	return nil
}

func (s *Master) Shutdown() {}
