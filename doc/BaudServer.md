# BaudServer

## Partition Operations

Create

Freeze

Delete

Backup

Restore


## Query Operation


## Mutate Operation


## RPC descriptions

service BaudServer {
	rpc CreatePartition(CreatePartitionReq) returns (CreatePartitionRes) {}
	rpc DeletePartition(DeletePartitionReq) returns (DeletePartitionRes) {}
	rpc FreezePartition(FreezePartitionReq) returns (FreezePartitionRes) {}
	rpc BackupPartition(BackupPartitionReq) returns (BackupPartitionRes) {}
	rpc RestorePartition(RestorePartitionReq) returns (RestorePartitionRes){}
	rpc ReplicatePartition(ReplicatePartitionReq) returns (ReplicatePartitionRes) {}
	rpc Query (QueryReq) returns (QueryRes) {}
	rpc Mutate (MutateReq) returns (MutateRes) {}
	rpc Info(InfoReq) returns (InfoRes) {}
}

## Start a BaudServer Replication Group

host5:$ baudserverd -http-addr host5:6001 -raft-addr host5:6002 -master http://host1:5001 -group 10001 ~/node

host6:$ baudserverd -http-addr host6:6001 -raft-addr host6:6002 -master http://host1:5001 -group 10001 ~/node

host7:$ baudserverd -http-addr host7:6001 -raft-addr host7:6002 -master http://host1:5001 -group 10001 ~/node


