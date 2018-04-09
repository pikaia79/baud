package document

import (
	"errors"
)

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

func (e *Entity) Document() (d *Document, err error) {
	if e == nil {
		return
	}
	if len(e.doc) == 0 {
		err = errors.New("invalid entity with empty doc")
		return
	}
	d = &Document{doc: e.doc}
	return
}

func (e *Entity) UID() (uid UID, err error) {
	if e == nil {
		return
	}
	uid = e.uid
	return
}

type Edge struct {
	src UID
	dst UID
	doc []byte
	version uint32
}

func NewEdge(src, dst UID, doc []byte) *Edge {
	return &Edge{src: src, dst: dst, doc: doc}
}

func (e *Edge) Document() (d *Document, err error) {
	if e == nil {
		return
	}
	d = &Document{doc: e.doc}
	return
}

func (e *Edge) Nodes() (src UID, dst UID, err error) {
	if e == nil {
		return
	}
	src = e.src
	dst = e.dst
	return
}

type Document struct {
	doc []byte
}

func NewDocument(doc []byte) *Document {
	return &Document{doc: doc}
}

func (d *Document) Doc() []byte {
	if d == nil {
		return nil
	}
	return d.doc
}
