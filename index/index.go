//the local storage & indexing engine for a partition
package index

import (
	"github.com/tiglabs/baud/schema"
)

type Index struct{}

func OpenIndex(path string) (i *Index, e error) {
	return
}

func (i *Index) AddSchema(predicates []schema.Predicate) error {
	return nil
}

func (i *Index) loadFromDisk(path string) error {
	return nil
}

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

func (i *Index) Stat() (*Stats, error) {
	return
}
