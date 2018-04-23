package server

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/gogo/protobuf/proto"

	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/proto/pspb"
)

func (s *Server) CreatePartition(ctx context.Context, request *pspb.CreatePartitionRequest) (*pspb.CreatePartitionResponse, error) {
	reponse := &pspb.CreatePartitionResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		reponse.Code = metapb.RESP_CODE_SERVERBUSY
		reponse.Message = "server is stopping"
	} else {
		s.adminEventCh <- request
	}

	return reponse, nil
}

func (s *Server) DeletePartition(ctx context.Context, request *pspb.DeletePartitionRequest) (*pspb.DeletePartitionResponse, error) {
	reponse := &pspb.DeletePartitionResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		reponse.Code = metapb.RESP_CODE_SERVERBUSY
		reponse.Message = "server is stopping"
	} else {
		s.adminEventCh <- request
	}

	return reponse, nil
}

func (s *Server) doPartitionCreate(p metapb.Partition) {
	partition := newPartition(s, p)
	if _, ok := s.partitions.LoadOrStore(p.ID, partition); ok {
		partition.Close()
	} else {
		for _, r := range p.Replicas {
			s.nodeResolver.addNode(r.NodeID, r.RaftAddrs)
		}

		partition.start()
	}
}

func (s *Server) doPartitionDelete(id metapb.PartitionID) {
	if p, ok := s.partitions.Load(id); ok {
		s.partitions.Delete(id)
		p.(*partition).Close()

		for _, r := range p.(*partition).meta.Replicas {
			s.nodeResolver.deleteNode(r.NodeID)
		}
	}

	path, _ := getPartitionPath(id, s.DataPath, false)
	os.RemoveAll(path)
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

func (s *Server) adminEventHandler() {
	for {
		select {
		case <-s.ctx.Done():
			return

		case event := <-s.adminEventCh:
			s.doAdminEvent(event)
		}
	}
}

func (s *Server) doAdminEvent(event proto.Message) {
	if s.stopping.Get() {
		return
	}

	switch e := event.(type) {
	case *pspb.CreatePartitionRequest:
		if p, ok := s.partitions.Load(e.Partition.ID); ok {
			if p.(*partition).meta.Status != metapb.PA_INVALID {
				return
			}

			p.(*partition).Close()
			s.partitions.Delete(e.Partition.ID)
		}

		s.doPartitionCreate(e.Partition)
		s.masterHeartbeat.trigger()

	case *pspb.DeletePartitionRequest:

	}
}
