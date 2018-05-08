package master

import (
	"github.com/tiglabs/baudengine/util/log"
	"sync"
)

type Master struct {
    config      *Config
    globalStore Store
    leaderCh    chan bool
    wg          sync.WaitGroup

    cluster   *Cluster
    apiServer *ApiServer
    rpcServer *RpcServer

    processorManager *ProcessorManager
    workerManager    *WorkerManager
    idGenerator      IDGenerator
    psRpcClient      PSRpcClient
}

func NewServer() *Master {
	return new(Master)
}

func (ms *Master) Start(config *Config) error {
	ms.config = config

    ms.globalStore = NewRaftStore(config)
    if err := ms.globalStore.Open(); err != nil {
        log.Error("fail to create raft store. err:[%v]", err)
        ms.Shutdown()
        return err
    }

    ms.cluster = NewCluster(config, ms.globalStore)
	if err := ms.cluster.Start(); err != nil {
		log.Error("fail to start cluster. err:[%v]", err)
        ms.Shutdown()
		return err
	}

	ms.rpcServer = NewRpcServer(config, ms.cluster)
	if err := ms.rpcServer.Start(); err != nil {
		log.Error("fail to start rpc server. err:[%v]", err)
        ms.Shutdown()
		return err
	}

	ms.apiServer = NewApiServer(config, ms.cluster)
	if err := ms.apiServer.Start(); err != nil {
		log.Error("fail to start api server. err:[%v]", err)
        ms.Shutdown()
		return err
	}

    ms.psRpcClient = GetPSRpcClientSingle(config)


    //ms.idGenerator = GetIdGeneratorSingle(ms.globalStore)
    //ms.processorManager = GetPMSingle(ms.cluster)
    //
	//ms.workerManager = NewWorkerManager(ms.cluster)
	//if err := ms.workerManager.Start(); err != nil {
	//	log.Error("fail to start worker manager. err:[%v]", err)
     //   ms.Shutdown()
	//	return err
	//}

	ms.watchLeader()

	return nil
}

func (ms *Master) Shutdown() {
    if ms.leaderCh != nil {
        close(ms.leaderCh)
        ms.leaderCh = nil
    }
	if ms.apiServer != nil {
		ms.apiServer.Close()
		ms.apiServer = nil
	}
	if ms.rpcServer != nil {
		ms.rpcServer.Close()
		ms.rpcServer = nil
	}
	if ms.workerManager != nil {
		ms.workerManager.Shutdown()
		ms.workerManager = nil
	}
	if ms.processorManager != nil {
        ms.processorManager.Close()
        ms.processorManager = nil
    }
    if ms.idGenerator != nil {
        ms.idGenerator.Close()
        ms.idGenerator = nil
    }
    if ms.psRpcClient != nil {
        ms.psRpcClient.Close()
        ms.psRpcClient = nil
    }
    if ms.cluster != nil {
		ms.cluster.Close()
		ms.cluster = nil
	}
    if ms.globalStore != nil {
        ms.globalStore.Close()
        ms.globalStore = nil
    }
    ms.wg.Wait()
}

func (ms *Master) watchLeader() {
    ms.leaderCh = make(chan bool, 4)
    ms.globalStore.WatchLeader(ms.leaderCh)

    ms.wg.Add(1)
    go func() {
        defer ms.wg.Done()
        
        for {
            select {
            case becomeLeader, opened := <-ms.leaderCh:
                if !opened {
                    log.Debug("closed leader watch channel")
                    return
                }

                if !becomeLeader {
                    if ms.workerManager != nil {
                        ms.workerManager.Shutdown()
                        ms.workerManager = nil
                    }
                    if ms.processorManager != nil {
                        ms.processorManager.Close()
                        ms.processorManager = nil
                    }
                    if ms.idGenerator != nil {
                        ms.idGenerator.Close()
                        ms.idGenerator = nil
                    }

                } else {

                    ms.cluster.Close()
                    if err := ms.cluster.Start(); err != nil {
                        log.Error("fail to restart cluster. err:[%v]", err)
                        break
                    }

                    if ms.idGenerator == nil {
                        ms.idGenerator = GetIdGeneratorSingle(ms.globalStore)
                    }
                    if ms.processorManager == nil {
                        ms.processorManager = GetPMSingle(ms.cluster)
                    }
                    if ms.workerManager == nil {
                        ms.workerManager = NewWorkerManager(ms.cluster)
                        if err := ms.workerManager.Start(); err != nil {
                            log.Error("fail to restart worker manager. err:[%v]", err)
                            break
                        }
                    }
                }
            }
        }
    }()
}
