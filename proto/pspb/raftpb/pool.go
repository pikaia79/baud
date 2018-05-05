package raftpb

import (
	"sync"
)

var raftCmdPool = &sync.Pool{
	New: func() interface{} {
		return new(RaftCommand)
	},
}

var writeCmdPool = &sync.Pool{
	New: func() interface{} {
		return new(WriteCommand)
	},
}

// CreateRaftCommand create a RaftCommand object
func CreateRaftCommand() *RaftCommand {
	return raftCmdPool.Get().(*RaftCommand)
}

// CreateWriteCommand create a WriteCommand object
func CreateWriteCommand() *WriteCommand {
	return writeCmdPool.Get().(*WriteCommand)
}

// Close reset and put to pool
func (c *RaftCommand) Close() error {
	if c.WriteCommand != nil {
		writeCmdPool.Put(c.WriteCommand)
		c.WriteCommand = nil
	}

	raftCmdPool.Put(c)
	return nil
}
