//the local storage & indexing engine for a partition
package kernel

import (
	"github.com/tiglabs/baud/schema"
)

type Index interface {
	Insert(uid schema.UID, document []byte) error
	Search(*Request) (*Result, error)
	Stat() (*Stats, error)
}

type Kernel struct{}

func (i *Kernel) Insert(uid schema.UID, document []byte) error {
	return nil
}

func (i *Kernel) Search(*Request) (*Result, error) {
	return
}

func (i *Kernel) Stat() (*Stats, error) {
	return
}
