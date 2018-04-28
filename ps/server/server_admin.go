package server

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/gogo/protobuf/proto"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
)

// CreatePartition admin grpc service for create partition
func (s *Server) CreatePartition(ctx context.Context, request *pspb.CreatePartitionRequest) (*pspb.CreatePartitionResponse, error) {
	response := &pspb.CreatePartitionResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"
	} else {
		s.adminEventCh <- request
	}

	return response, nil
}

// DeletePartition admin grpc service for delete partition
func (s *Server) DeletePartition(ctx context.Context, request *pspb.DeletePartitionRequest) (*pspb.DeletePartitionResponse, error) {
	response := &pspb.DeletePartitionResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	if s.stopping.Get() {
		response.Code = metapb.RESP_CODE_SERVER_STOP
		response.Message = "server is stopping"
	} else {
		s.adminEventCh <- request
	}

	return response, nil
}

// ChangeReplica admin grpc service for change replica of partition
func (s *Server) ChangeReplica(ctx context.Context, request *pspb.ChangeReplicaRequest) (*pspb.ChangeReplicaResponse, error) {
	response := &pspb.ChangeReplicaResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	return response, nil
}

// ChangeLeader admin grpc service for change leader of partition
func (s *Server) ChangeLeader(ctx context.Context, request *pspb.ChangeLeaderRequest) (*pspb.ChangeLeaderResponse, error) {
	response := &pspb.ChangeLeaderResponse{
		ResponseHeader: metapb.ResponseHeader{
			ReqId: request.ReqId,
			Code:  metapb.RESP_CODE_OK,
		},
	}

	return response, nil
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
