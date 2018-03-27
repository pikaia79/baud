# Roadmap of Baud Development

## First, Storage

A scalable document storage, just store, no search, no graph

Every json document has a builtin uint32 field, _slotid_, which indicates the partition (slot range) it will be located at. When it is inserted, the accepting PartitionServer (PS) raft group will allocate a UID for the document. 



