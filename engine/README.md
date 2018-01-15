# the storage engine for each partition

## API

schema operations: 

mutations:

insert/update/delete_object

query:

search_object

## Storage Layout

### RocksDB

a single rocksdb per store

### Metadata

#md#space_id#class_id#field_id -> <field type; index type>

### AutoIncrID Allocation

each Classs has its own auto-incr-id

### Objects

oid = <space_id, partition_id, class_id, auto_incr_id>

#ob#oid#field_id -> field_value


### Index

* In RocksDB

foreach uniquely-indexed field, field_value -> oid

<#ui#space_id.#class_id.#field_id.#field_value> -> oid

foreach commonly-indexed field, #field_value.#oid -> nil

<#ci#space_id.#class_id.#field_id.#field_value.#oid> -> nil

foreach fulltext-indexed field, #term.#oid -> nil

<#ft#analyzed#oid#field_id# -> analyzed terms (for fulltext-indexed fields)
<#ft#inverted#space_id.#class_id.#field_id.#term.#oid> -> nil






