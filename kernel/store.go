package kernel

import (
	"github.com/tiglabs/baud/schema"
)

type Store struct{}

func (i *Store) Insert(uid schema.UID, document []byte) error {
	return nil
}
