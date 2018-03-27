package schema

type DB struct {
	Name   string
	ID     uint32
	Spaces map[string]*Space
}

//each document has a uint32 field _slot_
type Space struct {
	Name    string
	ID      uint32
	Mapping []Field
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
