package ps

import (
	"github.com/tiglabs/baud/schema"
)

type Index struct{}

func (i *Index) AddEntity(o *schema.Entity) error {
	return nil
}

func (i *Index) DelEntity(oid shema.OID) error {
	return nil
}

func (i *Index) AddFact(fact schema.Fact) error {
	return nil
}

func (i *Index) DelFact(fact schema.Fact) error {
	return nil
}

func (i *Index) Search(*Request) (*Result, error) {
	return
}
