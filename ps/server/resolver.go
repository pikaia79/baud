package server

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/raft"
)

type nodeRef struct {
	refCount      int32
	heartbeatAddr string
	replicateAddr string
}

// RaftResolver resolve NodeID to net.Addr addresses
type RaftResolver struct {
	nodes *sync.Map
}

// NewRaftResolver create RaftResolver
func NewRaftResolver() *RaftResolver {
	return &RaftResolver{
		nodes: new(sync.Map),
	}
}

func (r *RaftResolver) AddNode(id metapb.NodeID, addrs metapb.ReplicaAddrs) {
	if id == 0 {
		return
	}

	ref := new(nodeRef)
	ref.heartbeatAddr = addrs.HeartbeatAddr
	ref.replicateAddr = addrs.ReplicateAddr
	obj, _ := r.nodes.LoadOrStore(id, ref)
	atomic.AddInt32(&(obj.(*nodeRef).refCount), 1)
}

func (r *RaftResolver) DeleteNode(id metapb.NodeID) {
	if obj, _ := r.nodes.Load(id); obj != nil {
		if count := atomic.AddInt32(&(obj.(*nodeRef).refCount), -1); count <= 0 {
			r.nodes.Delete(id)
		}
	}
}

func (r *RaftResolver) GetNode(id metapb.NodeID) (*nodeRef, error) {
	if obj, _ := r.nodes.Load(id); obj != nil {
		return obj.(*nodeRef), nil
	}
	return nil, fmt.Errorf("Cannot get node network information, nodeID=[%d]", id)
}

// NodeAddress resolve NodeID to net.Addr addresses.
func (r *RaftResolver) NodeAddress(nodeID uint64, stype raft.SocketType) (string, error) {
	node, err := r.GetNode(metapb.NodeID(nodeID))
	if err != nil {
		return "", err
	}

	switch stype {
	case raft.HeartBeat:
		return node.heartbeatAddr, nil
	case raft.Replicate:
		return node.replicateAddr, nil
	default:
		return "", fmt.Errorf("Unknown socket type[%v]", stype)
	}
}
