package gm

import (
	"github.com/tiglabs/baudengine/util/log"
	"sync"
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
	gm.wg.Wait()
}

func (gm *GM) watchLeader() {
	gm.leaderCh = make(chan *LeaderInfo, 4)

	gm.wg.Add(1)
	go func() {
		defer gm.wg.Done()

		for {
			select {
			case leaderChanging, opened := <-gm.leaderCh:
				if !opened {
					log.Debug("closed leader watch channel")
					return
				}

				if !leaderChanging.becomeLeader {
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
					gm.cluster.Close()
					if err := gm.cluster.Start(); err != nil {
						log.Error("fail to restart cluster. err:[%v]", err)
						break
					}

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
				}
			}
		}
	}()
}
