package schema

type DB struct {
	Name   string
	ID     uint32
	Spaces map[string]*Space
}

//each document has a uint32 field __slot
type Space struct {
	Name       string
	ID         uint32
	Mapping    []Field
	Categories []*Category

	ShardingPolicy ShardingPolicy
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

type Association struct {
	Subject    UID
	Predicate  String
	Object     UID
	MultiValue bool
}

type Category struct {
	ID           UID /// SpaceID = SlotID = 0
	Names        []string
	Superclasses []UID
	Subclasses   []UID
}

type ShardingPolicy struct {
	ShardingKey       string
	InitialPartitions int
}
