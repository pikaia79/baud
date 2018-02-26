package schema

type Space struct {
	ID   int
	Name string
}

type Class struct {
	ID           int
	Name         string
	Fields       map[string]Field
	PartitionKey string
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
	ClassID       uint16
	AutoIncrID    uint64
}

type Object struct {
	ID     OID
	Fields [][]byte
}
