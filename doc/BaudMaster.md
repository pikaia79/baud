# BaudMaster Architecture

three/five/.. BM instances form a replicated BM service, or leverage a distributed coordination service like etcd/consul to store the metadata of Baud itself. 

we currently choose the latter approach. 

## database metadata

schema (name)

space (name -> id)

partition (partition_id)

## cluster topo metadata

region, az, nodes/roles

baudmaster replicas

baudserver nodes

## change and remember partition replica states




