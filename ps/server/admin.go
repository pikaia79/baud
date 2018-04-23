package server

import (
	"io/ioutil"
	"os"
	"strconv"

	"github.com/tiglabs/baud/proto/metapb"
)

func (s *Server) doPartitionCreate(p metapb.Partition) {
	partition, ok := s.partitions.Load(p.ID)

}

func (s *Server) destroyExcludePartition(partitions []metapb.Partition) {
	dir, err := ioutil.ReadDir(s.DataPath)
	if err != nil {
		return
	}

	for _, fi := range dir {
		if fi.IsDir() {
			if id, err := strconv.ParseUint(fi.Name(), 10, 64); err == nil {
				delete := true
				for _, p := range partitions {
					if p.ID == id {
						delete = false
						break
					}
				}

				if delete {
					path, _ := getPartitionPath(id, s.DataPath, false)
					os.RemoveAll(path)
				}
			}
		}
	}
}

func (s *Server) reset() {
	os.RemoveAll(s.DataPath)
}
