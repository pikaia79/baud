package zm

import (
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"sync"
	"time"
)

type ZoneMaster struct {
	config *Config
	wg     sync.WaitGroup

	cluster   *Cluster
	apiServer *ApiServer
	rpcServer *RpcServer

	processorManager *ProcessorManager
	workerManager    *WorkerManager
	psRpcClient      PSRpcClient

	topoServer    *topo.TopoServer
	participation topo.MasterParticipation
}

func NewServer() *ZoneMaster {
	return new(ZoneMaster)
}

func (zm *ZoneMaster) Start(config *Config) error {
	zm.config = config

	zm.topoServer = topo.Open()

	zm.cluster = NewCluster(config, zm.topoServer)
	if err := zm.cluster.Start(); err != nil {
		log.Error("fail to start cluster. err:[%v]", err)
		zm.Shutdown()
		return err
	}

	zm.rpcServer = NewRpcServer(config, zm.cluster)
	if err := zm.rpcServer.Start(); err != nil {
		log.Error("fail to start rpc server. err:[%v]", err)
		zm.Shutdown()
		return err
	}

	zm.apiServer = NewApiServer(config, zm.cluster)
	if err := zm.apiServer.Start(); err != nil {
		log.Error("fail to start api server. err:[%v]", err)
		zm.Shutdown()
		return err
	}

	zm.psRpcClient = GetPSRpcClientSingle(config)

	var err error
	zm.participation, err = zm.topoServer.NewMasterParticipation(config.ClusterCfg.ZoneID, config.ClusterCfg.CurNodeId)
	if err != nil {
		return err
	}

	for {
		ctx, err := zm.participation.WaitForMastership()
		switch err {
		case nil:
			break
		case topo.ErrInterrupted:
			break
		default:
			//log.Errorf("Got error while waiting for master, will retry in 5s: %v", err)
			time.Sleep(5 * time.Second)
		}
	}

	return nil
}

func (zm *ZoneMaster) Shutdown() {
	if zm.apiServer != nil {
		zm.apiServer.Close()
		zm.apiServer = nil
	}
	if zm.rpcServer != nil {
		zm.rpcServer.Close()
		zm.rpcServer = nil
	}
	if zm.workerManager != nil {
		zm.workerManager.Shutdown()
		zm.workerManager = nil
	}
	if zm.processorManager != nil {
		zm.processorManager.Close()
		zm.processorManager = nil
	}
	if zm.psRpcClient != nil {
		zm.psRpcClient.Close()
		zm.psRpcClient = nil
	}
	if zm.cluster != nil {
		zm.cluster.Close()
		zm.cluster = nil
	}
	zm.participation.Stop()
}
