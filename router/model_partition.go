package router

import "github.com/tiglabs/baud/proto/metapb"

type Partition struct {
	meta metapb.Partition
	parent *Space
	psClient *PSClient
}

func (partition *Partition) Create(docJson []byte) metapb.Key {
	return nil
}

func (partition *Partition) Read(docId uint64) metapb.Value {
	return nil
}

func (partition *Partition) Update(docId uint64, value metapb.Value) error {
	return nil
}

func (partition *Partition) Delete(docId uint64) error {
	return nil
}
