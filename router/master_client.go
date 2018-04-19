package router

import (
	"github.com/tiglabs/baud/proto/masterpb"
	"sync"
)

type MasterClient struct {
	rpcClient masterpb.MasterRpcClient
}