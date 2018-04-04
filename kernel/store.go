package kernel

import (
	"baud/kernel/document"
	"baud/kernel/store"
)

type Store struct{
	docEngine   store.Store
}

func (i *Store) Insert(uid document.UID, document []byte) error {
	return nil
}
