package master

type PartitionPolicy struct {
	key           string
	function      string
	numPartitions int
}

type Space struct {
	entityOrEdge string
	name         string
	id           uint32

	partitioning PartitionPolicy
	mapping      []Field
}

type Field struct {
	Name        string
	Type        string
	Language    string
	IndexPolicy string
	MultiValue  bool
}
