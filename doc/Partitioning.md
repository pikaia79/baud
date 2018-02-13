# Partitioning Design

Baud gives applications fine-grained control over their data’s partitioning and locality.

storage limit, privision throughput

an object and its outgoing associations are located in the same partition

pre-allocate a given number of partitons, or allocate partitions on demand


## Partition Key Specification

1, if partition key = OID then partition_id = 0, 1, ... 

2, if partition key = some field, partition_id = hash of some field


## Dynamic Range-based Partitioning

e.g., partition ID = murmurhash(partition key)  ==> ranges ==> partitions

sorted partition IDs through all the partitions of a space

### Partition Names

schemaId-spaceId-partitionId[start-end], e.g. 0x08-0x03-0x0140-0x0180,


### Partition States

serving, 
splitting,
merging,
cleaning,

### Local Object ID allocation

each type of objects have their own auto incr id space

## Partition Splitting & Mergingg

a partition can be splitted into 2,3,4.. etc. any number of children

async filtered replication + quick cutover

partition merging is the reverse process of splitting


## Small Instances, Over-Allocation and Scheduling

small partition instances, say 2 core + 8G RAM + 64GB for a partition vs 64 core + 256G RAM + 2TB for a host

each host can over-commit locally and baudmaster schedules partition balancing globally

## Baremetal vs Cloud

## Durability through replication

### partition replica states

leader

follower

slave

backup

restore

## Consistency Model


