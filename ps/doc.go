/*

the PartitionSever implementation

PS

Container

RaftServer

Partition

PartialReplication

Request

Result


# UID allocation

UID = space_id|slot_id|auto_incr_id, the last element is allocated by PS leader and replicated via Raft.


# Partition Splitting

via partial replication



*/
package ps
