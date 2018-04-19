package server

import "github.com/tiglabs/baud/proto/masterpb"

type partition struct {
}

func (p *partition) getPartitionInfo() *masterpb.PartitionInfo {
	return nil
}

func (p *partition) Close() {
}
