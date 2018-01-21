# ContainerDB Data Model

## Observation

* nested entities 

* independent entities having n:n relationships

so adopt a hierarchical data model to model nested entities and edges to model relationships between independent entities. 

## Concepts

Field

Object

Class

Space

Edge

oid = <space_id (uint32), partition id (uint16), type_id(uint16), auto incr id (uint64)>

native graph: associations located together with source objects

nodes and edges in the graph are fully indexed. 

## Type System

objects, edges, and fields are all typed

## Index

unique, common, fulltext


## Change Streaming


