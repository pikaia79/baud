package schema

type Space struct {
	Name               string
	ID                 uint32
	PartitionAttribute string
}

type Graph struct {
	Name       string
	ID         uint32
	Spaces     map[string]Space
	Categories []Category
}

type OID struct {
	SpaceID       uint32
	PartitionHash uint32
	AutoIncrID    uint64
}

type PartitionID struct {
	Graph     uint32
	Space     uint32
	StartHash uint16
	EndHash   uint16
}

type Fact struct {
	Subject     OID
	Predicate   string
	Object      interface{}
	ObjType     int
	SingleValue bool
	IndexPolicy int
}

type Entity struct {
	Oid   OID
	Facts []Fact
}

//note that Category is also an entity in the #0 space located at the master.
type Category struct {
	Oid   OID
	Facts []Fact
}
