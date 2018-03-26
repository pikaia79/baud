package schema

type Graph struct {
	Name   string
	ID     uint32
	Spaces map[string]Space
}

type Space struct {
	Name string
	ID   uint32

	Partitioning PartitioningPolicy
	Mapping      []Field
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}

type PartitioningPolicy struct {
	Field       string
	ValueToSlot int //hash, value, etc.
}

type PartitionID struct {
	Graph     uint32
	Space     uint32
	StartSlot uint32
	EndSlot   uint32
}

type UID struct {
	SpaceID       uint32
	PartitionSlot uint32
	AutoIncrID    uint64
}

type Association struct {
	Subject    UID
	Predicate  String
	Object     UID
	MultiValue bool
}
