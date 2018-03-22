package schema

type Graph struct {
	Name   string
	ID     uint32
	Spaces map[string]Space
}

type Space struct {
	Name string
	ID   uint32

	partitionPolicy PartitionPolicy
	predicates      map[string]Predicate
	categories      map[string]*Category
}

type PartitionPolicy struct {
	Predicate   string
	ValueToSlot int //hash, value, etc.
}

type PartitionID struct {
	Graph     uint32
	Space     uint32
	StartSlot uint32
	EndSlot   uint32
}

//Unique ID for entities or categories
type UID struct {
	SpaceID       uint32
	PartitionSlot uint32
	AutoIncrID    uint64
}

type Predicate struct {
	Key         string
	ObjectType  uint8
	Sole        bool
	IndexPolicy uint8
}

type Fact struct {
	Subject   UID
	Predicate string
	Object    []byte
}

type Entity struct {
	Uid   UID
	Facts []Fact
}

type Category struct {
	Uid          UID //SpaceID = PartitionSlot = 0
	Name         string
	Superclasses []*Category
	Subclasses   []*Category
}
