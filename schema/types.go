package schema

type DB struct {
	Name   string
	ID     uint32
	Spaces map[string]*Space
}

type Space struct {
	Name         string
	ID           uint32 //odd number as entity space, even number as edge space.
	Mapping      []Field
	Partitioning PartitionPolicy
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}

type PartitionID struct {
	DB        uint32
	Space     uint32
	StartSlot uint32
	EndSlot   uint32
}

type UID struct {
	SpaceID    uint32
	SlotID     uint32
	AutoIncrID uint64
}

type PartitionPolicy struct {
	Key           string
	Function      string
	NumPartitions int
}
