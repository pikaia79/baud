//the local storage & indexing engine for a partition
package index

import (
	"github.com/tiglabs/baud/schema"
)

type Index struct{}

func OpenIndex(path string) (i *Index, e error) {
	return
}

func (i *Index) loadFromDisk(path string) error {
	return nil
}

func (i *Index) Mutate(*Mutation) error {
	return nil
}

func (i *Index) Search(*Request) (*Result, error) {
	return
}

func (i *Index) Stat() (*Stats, error) {
	return
}
