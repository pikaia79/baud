package gm

import (
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
)

var (
	topoServer *topo.TopoServer
)

type GM struct {
	config *Config
	wg     sync.WaitGroup

	cluster   *Cluster
	apiServer *ApiServer
	rpcServer *RpcServer

	processorManager    *ProcessorManager
	workerManager       *WorkerManager
	idGenerator         IDGenerator
	zoneMasterRpcClient ZoneMasterRpcClient
}

func NewServer() *GM {
	return new(GM)
}

func (gm *GM) Start(config *Config) error {
	gm.config = config
	topoServer = topo.Open()

	gm.cluster = NewCluster(config)
	if err := gm.cluster.Start(); err != nil {
		log.Error("fail to start cluster. err:[%v]", err)
		gm.Shutdown()
		return err
	}

	gm.rpcServer = NewRpcServer(config, gm.cluster)
	if err := gm.rpcServer.Start(); err != nil {
		log.Error("fail to start rpc server. err:[%v]", err)
		gm.Shutdown()
		return err
	}

	gm.apiServer = NewApiServer(config, gm.cluster)
	if err := gm.apiServer.Start(); err != nil {
		log.Error("fail to start api server. err:[%v]", err)
		gm.Shutdown()
		return err
	}

	gm.idGenerator = GetIdGeneratorSingle()
	gm.zoneMasterRpcClient = GetZoneMasterRpcClientSingle(config)
	// processorManager start() when init
	gm.processorManager = GetPMSingle(gm.cluster)

	gm.workerManager = NewWorkerManager(gm.cluster)
	if err := gm.workerManager.Start(); err != nil {
		log.Error("fail to start worker manager. err:[%v]", err)
		gm.Shutdown()
		return err
	}

	gm.watchLeader()

	return nil
}

func (gm *GM) Shutdown() {
	if gm.apiServer != nil {
		gm.apiServer.Close()
		gm.apiServer = nil
	}
	if gm.rpcServer != nil {
		gm.rpcServer.Close()
		gm.rpcServer = nil
	}
	if gm.workerManager != nil {
		gm.workerManager.Shutdown()
		gm.workerManager = nil
	}
	if gm.processorManager != nil {
		gm.processorManager.Close()
		gm.processorManager = nil
	}
	if gm.idGenerator != nil {
		gm.idGenerator.Close()
		gm.idGenerator = nil
	}
	if gm.zoneMasterRpcClient != nil {
		gm.zoneMasterRpcClient.Close()
		gm.zoneMasterRpcClient = nil
	}
	if gm.cluster != nil {
		gm.cluster.Close()
		gm.cluster = nil
	}
	if topoServer != nil {
		topoServer.Close()
		topoServer = nil
	}
	gm.wg.Wait()
}

func (gm *GM) watchLeader() {

	gm.wg.Add(1)
	go func() {
		defer gm.wg.Done()

		//TODO 调用global etcd进行gm leader选举, 接口由@杨洋提供

		for {
			select {

			case "":

				if "leader2follower" == "" {
					gm.cluster.isGMLeader = false
					gm.cluster.currentGMLeaderNodeID = 0
					gm.cluster.currentGMLeaderAddr = ""
					if gm.workerManager != nil {
						gm.workerManager.Shutdown()
						gm.workerManager = nil
					}
					if gm.processorManager != nil {
						gm.processorManager.Close()
						gm.processorManager = nil
					}
					if gm.idGenerator != nil {
						gm.idGenerator.Close()
						gm.idGenerator = nil
					}
				} else {
					if gm.idGenerator == nil {
						gm.idGenerator = GetIdGeneratorSingle()
					}
					if gm.processorManager == nil {
						gm.processorManager = GetPMSingle(gm.cluster)
					}
					if gm.workerManager == nil {
						gm.workerManager = NewWorkerManager(gm.cluster)
						if err := gm.workerManager.Start(); err != nil {
							log.Error("fail to restart worker manager. err:[%v]", err)
							break
						}
					}
					gm.cluster.isGMLeader = true
					gm.cluster.currentGMLeaderNodeID = 0
					gm.cluster.currentGMLeaderAddr = ""
				}
			}
		}
	}()
}
