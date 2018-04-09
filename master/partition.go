package master

type Partition struct {
	id           string // equals replGroup id
	entityOrEdge string
	db           uint32
	space        uint32
	startSlot    uint32
	endSlot      uint32

	replGroup uint32

	//for splitting & merging
	leftCh  *Partition
	rightCh *Partition
	parent  *Partition

	status string //serving, splitting, cleaning, etc.
}

type ReplGroup struct {
	id       uint32
	replicas []PartitionServer
}

type PartitionServer struct {
	id   uint32 // equals ip
	role string
	zone string
	ip   string
	port string

	cpu    int
	memory int
	disk   int
}
