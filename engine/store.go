package engine

import (
	"proto"
)

type Store struct {}

func NewStore(path string) (s *Store, e erro) {}

func (s *Store) DefineSchema(fid proto.FID, valueType schema.Type, indexType schema.Index) error {
	return nil
}

func (s *Store) InsertObject(object *proto.Object) (oid schema.OID, e error) {}

func (s *Store) UpdateObject(object *proto.Object) error {}

func (s *Store) DeleteObject(oid shema.OID) error {}

func (s *Store) GetObjectByOID(oid proto.OID, fields []uint16) (o schema.Object, e error) {}

func (s *Store) EqualQuery(fid proto.FID, value {}interface) (oids []schema.OID, e error) {}

func (s *Store) RangeQuery(fid proto.FID, start, end {}interface) (oids []schema.OID, e error) {}


