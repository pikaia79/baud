package master

import (
	//"golang.org/x/net/context"
	"util/log"
)

type Master struct {
	config			*Config

	cluster			*Cluster

	apiServer		*ApiServer
	rpcServer		*RpcServer
	//ctx 			context.Context
	//ctxCancel		context.CancelFunc

}

func NewServer() *Master {
	return new(Master)
}

func (ms *Master) Start(config *Config) error {
	ms.config = config
	//ms.ctx, ms.ctxCancel = context.WithCancel(context.Background())

	cluster := NewCluster()
	if err := cluster.Start(config); err != nil {
		log.Error("fail to start cluster. err:[%v]", err)
		return err
	}

	ms.apiServer = NewApiServer(config)
	ms.rpcServer = NewRpcServer()


	return nil
}

func (ms *Master) Shutdown() {
	if ms.cluster != nil {
		ms.cluster.Close()
	}
	if ms.apiServer != nil {
		ms.apiServer.Close()
	}
}
