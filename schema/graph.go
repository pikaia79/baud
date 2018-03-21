package schema

type Space struct {
	Name               string
	ID                 uint32
	PartitionPredicate string
	Categories         map[string]*Category
}

type Graph struct {
	Name   string
	ID     uint32
	Spaces map[string]Space
}

type PartitionID struct {
	Graph     uint32
	Space     uint32
	StartHash uint32
	EndHash   uint32
}

//Unique ID for entities or categories
type UID struct {
	SpaceID       uint32
	PartitionHash uint32
	AutoIncrID    uint64
}

type PredicateSignature struct {
	Key         string
	ObjectType  uint8
	Sole        bool
	IndexPolicy uint8
}

type Fact struct {
	Subject   UID
	Predicate PredicateSignature
	Object    []byte
}

type Entity struct {
	Uid   UID
	Facts []Fact
}

type Category struct {
	Uid          UID //SpaceID = PartitionHash = 0
	Name         string
	Superclasses []*Category
	Subclasses   []*Category
}
