//the local storage & indexing engine for a partition
package kernel

import (
	"github.com/tiglabs/baud/kernel/document"
)

type Index interface {
	Insert(uid document.UID, document []byte) error
	Search(*Request) (*Result, error)
	Stat() (*Stats, error)
}

type Kernel struct{}

func (i *Kernel) Insert(uid document.UID, document []byte) error {
	return nil
}

func (i *Kernel) Search(*Request) (*Result, error) {
	return
}

func (i *Kernel) Stat() (*Stats, error) {
	return
}
