package kernel

type UID struct {
	SpaceID    uint32
	SlotID     uint32
	AutoIncrID uint64
}

type Entity struct {
	uid UID
	doc []byte
}

func (e *Entity) Document() (d *Document, e error) {
	return
}

func (e *Entity) UID() (uid UID, e error) {
	return
}

type Edge struct {
	src UID
	dst UID
	doc []byte
}

func (e *Edge) Document() (d *Document, e error) {
	return
}

func (e *Edge) Nodes() (src UID, dst UID, e error) {
	return
}

type Document struct {
	doc []byte
}
