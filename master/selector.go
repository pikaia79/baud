package master

type Selector interface {
	SelectTarget(servers []*PartitionServer, minNum ...int) []*PartitionServer
}
