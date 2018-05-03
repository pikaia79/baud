package raftpb

import (
	"sync"
)

var raftCmdPool = &sync.Pool{
	New: func() interface{} {
		return new(RaftCommand)
	},
}

// CreateRaftCommand create a RaftCommand object
func CreateRaftCommand() *RaftCommand {
	return raftCmdPool.Get().(*RaftCommand)
}

// Close reset and put to pool
func (c *RaftCommand) Close() error {
	c.DataRequest = nil
	raftCmdPool.Put(c)

	return nil
}
