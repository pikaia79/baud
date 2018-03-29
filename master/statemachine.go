package master

import (
	"github.com/tiglabs/raft"
)

type StateMachine struct {
	id      uint64
	applied uint64
	raft    *raft.RaftServer
}
