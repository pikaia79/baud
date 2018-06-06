package raftstore

import "github.com/tiglabs/baudengine/proto/metapb"

type EventListener interface {
	HandleRaftReplicaEvent(event *RaftReplicaEvent)
	HandleRaftLeaderEvent(event *RaftLeaderEvent)
	HandleRaftFatalEvent(event *RaftFatalEvent)
}

type RaftReplicaEvent struct {
	Delete  bool
	Replica *metapb.Replica
}

type RaftLeaderEvent struct {
	Store *Store
}

type RaftFatalEvent struct {
	Store *Store
	Cause error
}
