package server

import (
	"os"

	"github.com/tiglabs/baud/proto/metapb"
)

func (s *Server) createPartition(p *metapb.Partition) {

}

func (s *Server) destroyPartition(id metapb.PartitionID) {
	os.RemoveAll(getPartitionPath(id, s.DataPath))
}

func (s *Server) destroyAll() {
	os.RemoveAll(s.DataPath)
	if err := os.MkdirAll(s.DataPath, os.ModePerm); err != nil {
		panic(err)
	}
}
