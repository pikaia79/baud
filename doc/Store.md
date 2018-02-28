# Storage Engine of Each Partition

## API

schema operations: 

define_otype/etype

mutations:

insert/update/delete_object/edge

query:

search_object/edge()


## Storage Layout

each Partition Replica has a 'Store' instance. 

### RocksDB

a single rocksdb per partiton

### Metadata

#md#space_id#class_id#field_id -> <field type; index type>

### AutoIncrID Allocation

Q: per-otype or per-partition space? 

A: PS remembers each otype's current auto incr id


### Objects

oid = <space_id, partition_id, otype_id, auto incr id>

#ob#oid#field_id -> field_value


### Edges

<#ed#oid#label#dst> -> timestamp


### Index

* In RocksDB

foreach uniquely-indexed field, field_value -> oid

<#ui#space_id.#otype_id.#field_id.#field_value> -> oid

foreach commonly-indexed field, #field_value.#oid -> nil

<#ci#space_id.#otype_id.#field_id.#field_value.#oid> -> nil

foreach fulltext-indexed field, #term.#oid -> nil

<#ft#analyzed#oid#field_id# -> analyzed terms (for fulltext-indexed fields)
<#ft#inverted#space_id.#otype_id.#field_id.#term.#oid> -> nil





