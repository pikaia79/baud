package store

import (
	"schema"
)

type Store struct {}

func NewStore(path string) (s *Store, e erro) {}

func (s *Store) DefineSchema(fid schema.FID, valueType schema.Type, indexType schema.Index) error {
	return nil
}

func (s *Store) InsertObject(object *schema.Object) (oid schema.OID, e error) {}

func (s *Store) UpdateObject(object *schema.Object) error {}

func (s *Store) DeleteObject(oid shema.OID) error {}

func (s *Store) GetObjectByOID(oid schema.OID, fields []uint16) (o schema.Object, e error) {}

func (s *Store) EqualQuery(fid schema.FID, value {}interface) (oids []schema.OID, e error) {}

func (s *Store) RangeQuery(fid schema.FID, start, end {}interface) (oids []schema.OID, e error) {}


