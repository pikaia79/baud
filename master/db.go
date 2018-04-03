package master

type DB struct {
	name string
	id   uint32

	spaceNameMap map[string]uint32
	spaces       map[uint32]*Space
}

//functions
