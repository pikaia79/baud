package client

import (
	"proto"
)

type BaudStorage struct {}

func (bs *BaudStorage) OpenDatabase(db string) error {}

func (bs *BaudStorage) CreateSpace(spaceSchema *proto.Space) error {}

func (bs *BaudStorage) InsertObject(space string, object *Object) error {}

func (bs *BaudStorage) UpdateObject(space string, object Object) error {}

func (bs *BaudStorage) AddEdge(src, dst OID) error {}


