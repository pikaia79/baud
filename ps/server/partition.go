package server

import (
	"errors"
	"fmt"

	"github.com/tiglabs/baudengine/engine"
	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/proto/pspb"
)

type PartitionStore interface {
	Start()
	Close() error
	GetMeta() metapb.Partition
	GetStats() *masterpb.PartitionInfo

	Get(docID engine.DOC_ID, timeout string) (doc engine.DOCUMENT, found bool, err error)

	Bulk(requests []pspb.RequestUnion, timeout string) (responses []pspb.ResponseUnion, err error)
}

// PartitionStoreBuilder is used to build the PartitionStore.
type PartitionStoreBuilder func(server *Server, meta metapb.Partition) (PartitionStore, error)

var (
	partitionStores = make(map[string]PartitionStoreBuilder, 8)

	errorNameInvalid = errors.New("Registration name is invalid")
)

// RegisterPartitionStore is used to register the PartitionStore implementers in the initialization phase.
func RegisterPartitionStore(name string, builder PartitionStoreBuilder) {
	if name == "" || builder == nil {
		panic("Registration name and builder cannot be empty")
	}
	if _, ok := partitionStores[name]; ok {
		panic(fmt.Sprintf("Duplicate registration engine name for %s", name))
	}

	partitionStores[name] = builder
}

// BuildPartitionStore create an PartitionStore based on the specified name.
func BuildPartitionStore(name string, server *Server, meta metapb.Partition) (s PartitionStore, err error) {
	if name == "" {
		return nil, errorNameInvalid
	}

	if builder := partitionStores[name]; builder != nil {
		s, err = builder(server, meta)
	} else {
		err = fmt.Errorf("Registered engine[%s] does not exist", name)
	}
	return
}

// ExistPartitionStore return whether the PartitionStore exists.
func ExistPartitionStore(name string) bool {
	if builder := partitionStores[name]; builder != nil {
		return true
	}

	return false
}
