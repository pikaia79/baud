package object

type UID struct {
	SpaceID    uint32
	SlotID     uint32
	AutoIncrID uint64
}

func (u *UID) Valid() bool {
	if u == nil {
		return false
	}
	if u.SpaceID > 0 {
		return true
	}
	return false
}


type Entity struct {
	uid UID
	doc []byte
	version uint32
}

func NewEntity(uid UID, doc []byte) *Entity {
	return &Entity{uid: uid, doc: doc}
}
