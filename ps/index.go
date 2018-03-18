package ps

import (
	"github.com/tiglabs/baud/schema"
)

type Index struct{}

func (i *Index) Index(o *schema.Entity) error {
	return nil
}

func (i *Index) Delete(oid shema.OID) error {
	return nil
}

func (i *Index) Entity(oid schema.OID) (*Entity, error) {
	return
}

func (i *Index) Search(*Request) (*Result, error) {
	return
}
