# The Architecture of BaudEngine

BaudEngine is distributed database engine with elastic storage and flexible search.

## Data Model

Field, Object, Space, DB

Field: key -> a typed value or a sorted array of values

Any field can be indexed and morever full-text search is a first-class citizen. 

* Schema

A scalar field is defined as the primary key and a scalar field as the partition key (route key, 16bit integer). 

Other fields are either single-value or multi-value.

## Concepts

BaudEngine is a multi-datacenter distributed system. However, it can be run in the single-datacenter mode.  

### zone

Zones, or called cells, are the unit of physical isolation as well as admin deployment. 

### components

* Master

the global master of the entire cluster

the zone master per zone

both are based on raft replication for high availability

* Partition Server

the writer role, i.e. Writer Partition Server (WPS)

the reader role, i.e. Reader Partition Server (RPS)

* Client

the Go SDK directly talking with masters and PSes.

* API Layer

BaudSQL - the NewSQL gateway with MySQL-compatible protocol

BaudSearch - the NoSQL gateway with ElasticSearch-compatible protocol

### software stack

* cluster management

conainer-native - BaudEngine can be run in Kubernetes, or on bare metal. 

each zone has a DCOS/Kubernetes interface to allocate PS nodes and router nodes. 

each DB is allocated with its own set of PS across different zones.

* BaudStorage

As the shared datacenter storage, BS is mounted to store partition data, which is log-structured sorted key-value files and redo-logs (WALs). 

Note BaudEngine can also run on local filesystems - actually BS is transparent to BE. 

### partitioning 

db -> space -> partition = partition key range

### replication

* cross-zone replication

Each partition has a Raft state machine located in three 'writer-role' partitionservers of different zones. Actually the writer-role partitionservers run the 'multi-raft' protocol. 

* within-zone replication

In any zone, a partition can have any number of read-only replicas, which are served by the 'Reader-Role' partitionservers reading the underlying Baudstorage files. 

### re-sharding

Note that all the partition data is persistent on the BaudStorage distributed filesystem - actually BaudEngine is a pure computing system seperated with storage. So we leverage it to do partition splitting in an easier way. 

However, we recommand that a space is pre-sharded and over-sharded to avoid re-sharding on runtime.

### scalability guarantee

One Baud cluster can host one to thousands of databases; 
one DB can host billions of spaces;
one space can host unlimited number of objects;

### local index vs. global index

currently local index only. 

## Master

There are two tiers for the cluster topo metadata managment. 

### globalmaster

* core data structures

db and space schemas

zones

partitions -> zones

note that the globalmaster has no knowledge of nodes, neither PS nodes nor router nodes

* key operations

create DB and Spaces

split or merge a partition

### zonemaster

* core data structures

partitions -> PS nodes

* key operations

assign partitions to PS nodes

* failure detector (FD)

distributed voting of PS health

* placement driver (PD)

move partitions between PS nodes for load balancing

### availability & reliability

raft replication

data structures are in memory but also marshalled and written to the underlying key-value store


## PS

there are two PS roles, i.e. two modes of running instances: 

* the writer role. participating multi-raft, a raft statemachine per partition, and logging the raft operations locally 

* the reader role. just reading from BaudStorage to serve partition read-only requests

### partition data store

Each partition has a single key-value storage engine for both objects and indexes, which is persisted onto the Baudstorage datacenter filesystem

* the primary index

(#PI, primaryKey, fieldName, fieldValue) -> timestamp

* secondary indexes

(#SI, fieldName, fieldValue or term, primaryKey) -> timestamp

### Synonym Table

term synonyms are stored as a file of Baudstorage and loaded by PS for document analysis.

### Key Operations


## Client

The client needs to interact with not only the zonemaster of its own zone but also the zonemasters of other zones. 

## API Layers

### BaudSQL 

Tables sharing the same partition key = one space

### BaudSearch

JSON <--> Object

ranking

### BaudGraph

TODO: the query language


## Deployment Flexibility

* single zone or multiple zones

* the BaudStorage datacenter filesystem or local filesystems

* Kubernetes or bare metal


## Manageability

Ops Center

Dashboard

### Monitoring

cluster-level statistics

space-level info

individual nodes

GC

SlowLog

### Upgrade


## Applications

products

email

messaging

blogging


