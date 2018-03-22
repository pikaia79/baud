# the PartitionIndex internals

## for the metadata i.e. the predicate schema

predicatekey -> object type, index policy, sole value or not, ..


## for the fact data part

<#UID, predicatekey> -> predicatemeta

<#UID, predicatekey, 0> -> object0

if it is not sole value: 

<#UID, predicatekey, i> -> object(i) where i = 1, 2, ...


## for the fact index part

<predicatekey, object value or term or fuzzy term> -> #UID


## search request

## predicate query

numberic range, string match, phrase match, ..

## analysis

value -> token -> term


