package schema

type ObjectType struct {
	ID     int
	Name   string
	Fields map[string]Field
}

type EdgeType struct {
	ID     int
	Name   string
	Src    *ObjectType
	Dst    *ObjectType
	Fields map[string]Field
}

type Database struct {
	ID          int
	Name        string
	Spaces      map[string]*Space
	Assocations map[string]*EdgeType
	Classes     map[string]*ObjectType
}

type Space struct {
	ID           int
	Name         string
	PartitionKey string
	Classes      map[string]*ObjectType
}

type Field struct {
	ID        int
	Name      string
	ValueType string
	IndexType string
}

type OID struct {
	SpaceID       uint32
	PartitionHash uint16
	TypeID        uint16
	AutoIncrID    uint64
}

type Object struct {
	ID     OID
	Fields map[string][]byte
}

type EID struct {
	TypeID uint32
	Source OID
	Destin OID
}

type Edge struct {
	ID     EID
	Fields map[string][]byte
}

type PartitionID struct {
	DB        uint32
	Space     uint32
	StartHash uint16
	EndHash   uint16
}
