package server

import (
	"fmt"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
	"github.com/tiglabs/baudengine/ps/storage/raftstore"
)

type PartitionStore interface {
	Start()
	Close() error
	GetMeta() metapb.Partition
	GetStats() *masterpb.PartitionInfo

	Get(docID engine.DOC_ID, timeout string) (doc engine.DOCUMENT, found bool, err error)

	Bulk(requests []pspb.RequestUnion, timeout string) (responses []pspb.ResponseUnion, err error)
}

func (s *Server) CreatePartitionStore(p metapb.Partition) (PartitionStore, error) {
	switch s.PartitionStore {
	case "raftstore":
		dataPath, raftPath, err := s.meta.getDataAndRaftPath(p.ID)
		if err != nil {
			return nil, err
		}

		conf := new(raftstore.StoreConfig)
		conf.EngineConfig = engine.EngineConfig{
			Path:         dataPath,
			ReadOnly:     false,
			ExtraOptions: s.StoreOption,
		}
		conf.EngineName = s.StoreEngine
		conf.Meta = p
		conf.NodeID = s.NodeID
		conf.RaftPath = raftPath
		conf.RaftConfig = s.raftConfig
		conf.RaftServer = s.raftServer
		conf.EventListener = s
		return raftstore.CreateStore(s.ctx, conf), nil

	default:
		return nil, fmt.Errorf("unsupport partition store type: %s", s.PartitionStore)
	}
}

func (s *Server) HandleRaftReplicaEvent(event *raftstore.RaftReplicaEvent) {
	if event.Delete {
		s.raftResolver.DeleteNode(event.Replica.NodeID)
	} else {
		s.raftResolver.AddNode(event.Replica.NodeID, event.Replica.ReplicaAddrs)
	}
}

func (s *Server) HandleRaftLeaderEvent(event *raftstore.RaftLeaderEvent) {
	s.masterHeartbeat.trigger()
}

func (s *Server) HandleRaftFatalEvent(event *raftstore.RaftFatalEvent) {
	s.masterHeartbeat.trigger()
}
