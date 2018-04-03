package master

type Partition struct {
	entityOrEdge string
	db           uint32
	space        uint32
	startSlot    uint32
	endSlot      uint32

	replicas []*Replica

	//for splitting & merging
	leftCh  *Partition
	rightCh *Partition
	parent  *Partitin

	status string //serving, splitting, cleaning, etc.
}

type Replica struct {
	role string
	zone string
	ip   string
	port string
}
