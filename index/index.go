//the local storage & indexing engine for a partition
package index

import (
	"github.com/tiglabs/baud/schema"
)

type Index struct{}

//note: o.UID is also given by the PS
func (i *Index) AddEntity(o *schema.Entity) error {
	return nil
}

func (i *Index) DelEntity(uid schema.UID) error {
	return nil
}

func (i *Index) AddFacts(facts []chema.Fact) error {
	return nil
}

func (i *Index) DelFacts(facts []schema.Fact) error {
	return nil
}

func (i *Index) Search(*Request) (*Result, error) {
	return
}
