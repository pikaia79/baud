package server

import (
	"fmt"
	"sync"

	"github.com/tiglabs/baud/proto"
	"github.com/tiglabs/baud/proto/pspb"
	"github.com/tiglabs/raft"
)

type nodeRef struct {
	node     *pspb.Node
	refCount int
}

// NodeResolver resolve NodeID to net.Addr addresses
type NodeResolver struct {
	sync.RWMutex
	nodes map[proto.NodeID]*nodeRef
}

// NewNodeResolver create NodeResolver
func NewNodeResolver() *NodeResolver {
	return &NodeResolver{
		nodes: make(map[proto.NodeID]*nodeRef, 64),
	}
}

func (r *NodeResolver) addNode(node *pspb.Node) {
	if node == nil {
		return
	}

	r.Lock()
	defer r.Unlock()

	if obj, ok := r.nodes[node.ID]; ok {
		obj.refCount++
	} else {
		r.nodes[node.ID] = &nodeRef{node: node, refCount: 1}
	}
}

func (r *NodeResolver) deleteNode(id proto.NodeID) {
	r.Lock()
	defer r.Unlock()

	if obj, ok := r.nodes[id]; ok {
		obj.refCount--
		if obj.refCount <= 0 {
			delete(r.nodes, id)
		}
	}
}

func (r *NodeResolver) getNode(id proto.NodeID) (*pspb.Node, error) {
	r.RLock()
	defer r.RUnlock()

	if obj, ok := r.nodes[id]; ok {
		return obj.node, nil
	}
	return nil, fmt.Errorf("Cannot get node network information, nodeID=[%d]", id)
}

// NodeAddress resolve NodeID to net.Addr addresses.
func (r *NodeResolver) NodeAddress(nodeID proto.NodeID, stype raft.SocketType) (string, error) {
	node, err := r.getNode(nodeID)
	if err != nil {
		return "", err
	}

	switch stype {
	case raft.HeartBeat:
		return node.RaftAddrs.HeartbeatAddr, nil
	case raft.Replicate:
		return node.RaftAddrs.ReplicateAddr, nil
	default:
		return "", fmt.Errorf("Unknown socket type[%v]", stype)
	}
}
