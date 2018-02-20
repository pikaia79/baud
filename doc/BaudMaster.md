# BaudMaster Architecture

three/five/.. BM instances form a replicated BM service, or leverage a distributed coordination service like etcd/consul to store the metadata of Baud itself. 

we currently choose the latter approach. 

## database metadata

space (name -> id)

class (name -> id)

partition (partitionhash range)


## cluster topo metadata

region, cell, rack, nodes

baudmaster nodes (roles)

baudserver nodes (roles)

## baudmaster key operations

### Create Space

0, foreach partition among the space

1, call JDOS to start several baudserver nodes;

2, ask the baudserver nodes to form a raft group as well as optional async replicas

3, call the raft leader to create a partition


### Split Partition

0, call JDOS to start baudserver nodes

1, call the nodes to form two new raft groups

2, call the two raft leaders to setup async filtered replication with the original to-be-splitted partition leader

3, replicate

4, cutover

### Merge Partition

0, call JDOS to start baudserver nodes

1, call the nodes to form a new raft groups

2, call the raft leader to setup async replication with the original to-be-merged partition leaders

3, replicate

4, cutover


## Start a controller

host2:$ baud -cm -http-addr host2:5001 -raft-addr host2:5002 -topo http://host1:5001 -data ~/node



