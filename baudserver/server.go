package baudserver

import (
	"engine"
	"proto"
)

type Server struct {
	p *Partition

	info *proto.ServerInfo
}

func (s *Server) createPartition() {}


