# the PartitionIndex internals


## for the fact data part

<#UID, predicatekey> -> metadata (object type, index policy, sole value or not) + packed object values

## for the fact index part

<predicatekey, object value or term or fuzzy term> -> #UID


## search request

## predicate query

numberic range, string match, phrase match, ..

## analysis

value -> token -> term


