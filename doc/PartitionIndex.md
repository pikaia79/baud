# the PartitionIndex internals

## for the fact data part

<#UID, predicatekey> -> predicatemeta

<#UID, predicatekey, 0> -> object0

if it is not sole value: 

<#UID, predicatekey, i> -> object(i) where i = 1, 2, ...


## for the fact index part

<predicatekey, object value or term or fuzzy term> -> #UID



