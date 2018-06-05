package gm

import (
	"github.com/tiglabs/baudengine/topo"
	"github.com/tiglabs/baudengine/util/log"
	"golang.org/x/net/context"
	"strings"
	"sync"
	"time"
)

const ETCD_TIMEOUT = 5 * time.Second

var (
	TopoServer *topo.TopoServer
)

type GM struct {
	config *Config
	wg     sync.WaitGroup

	cluster   *Cluster
	apiServer *ApiServer
	rpcServer *RpcServer

	isGMLeader            bool
	currentGMLeaderNodeID string
	currentGMLeaderAddr   string

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
	topoServer, err := topo.OpenServer(gm.config.ClusterCfg.Topo, gm.config.ClusterCfg.TopoEndPoints, gm.config.ClusterCfg.TopoRootDir)
	if err != nil {
		log.Error("Fail to create topo server. err[%v]", err)
		return err
	}
	TopoServer = topoServer

	ctx, _ := context.WithTimeout(context.Background(), ETCD_TIMEOUT)

	gm.cluster = NewCluster(ctx, gm.config, gm)
	if err := gm.cluster.Start(); err != nil {
		log.Error("fail to start cluster. err:[%v]", err)
		gm.Shutdown()
		return err
	}

	gm.apiServer = NewApiServer(gm.config, gm.cluster)
	if err := gm.apiServer.Start(); err != nil {
		log.Error("fail to start api server. err:[%v]", err)
		gm.Shutdown()
		return err
	}

	gm.rpcServer = NewRpcServer(gm.config, gm.cluster)
	if err := gm.rpcServer.Start(); err != nil {
		log.Error("fail to start rpc server. err:[%v]", err)
		gm.Shutdown()
		return err
	}
	gm.newMasterParticipation()
	return nil
}

func (gm *GM) Shutdown() {
	if gm.workerManager != nil {
		gm.workerManager.Close()
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
	if gm.rpcServer != nil {
		gm.rpcServer.Close()
		gm.rpcServer = nil
	}
	if gm.apiServer != nil {
		gm.apiServer.Close()
		gm.apiServer = nil
	}
	if gm.cluster != nil {
		gm.cluster.Close()
		gm.cluster = nil
	}
	if TopoServer != nil {
		TopoServer.Close()
		TopoServer = nil
	}
	gm.wg.Wait()
}

func (gm *GM) newMasterParticipation() {
	ctx, cancel := context.WithTimeout(context.Background(), ETCD_TIMEOUT)
	defer cancel()

	masterParticipation, err := TopoServer.NewMasterParticipation(
		topo.GlobalZone,
		gm.config.ClusterCfg.GmNodeIp+":"+gm.config.ClusterCfg.GmNodeId)
	if err != nil {
		log.Error("fail to invoke TopoServer new master participation. err:[%v]", err)
		return
	}
	go func() {
		for {
			cmID, err := masterParticipation.GetCurrentMasterID(ctx)
			if err != nil {
				log.Error("fail to get current master ID. err:[%v]", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if cmID == "" || cmID != gm.config.ClusterCfg.GmNodeIp+":"+gm.config.ClusterCfg.GmNodeId {
				// last time I'm the gm leader, but now I'm not.
				if gm.isGMLeader == true {
					gm.isGMLeader = false
					gm.currentGMLeaderNodeID = ""
					gm.currentGMLeaderAddr = ""
					if gm.workerManager != nil {
						gm.workerManager.Close()
						gm.workerManager = nil
					}
					if gm.processorManager != nil {
						gm.processorManager.Close()
						gm.processorManager = nil
					}
					if gm.zoneMasterRpcClient != nil {
						gm.zoneMasterRpcClient.Close()
						gm.zoneMasterRpcClient = nil
					}
					if gm.idGenerator != nil {
						gm.idGenerator.Close()
						gm.idGenerator = nil
					}
				}

				ctxMastership, err := masterParticipation.WaitForMastership()
				switch err {
				case nil:
					gm.isGMLeader = true
					gm.currentGMLeaderNodeID = gm.config.ClusterCfg.GmNodeIp + ":" + gm.config.ClusterCfg.GmNodeId
					gm.currentGMLeaderAddr = gm.config.ClusterCfg.GmNodeIp

					gm.idGenerator = GetIdGeneratorSingle()
					gm.zoneMasterRpcClient = GetZoneMasterRpcClientSingle(gm.config)
					gm.processorManager = GetPMSingle(gm.cluster)
					gm.processorManager.Start()
					gm.workerManager = NewWorkerManager(gm.cluster)
					gm.workerManager.Start()
					select {
					case <-ctxMastership.Done():
						log.Info("Last time I'm the gm leader, but ctxMastership.Done invoked.")
						continue
					}
				case topo.ErrInterrupted:
					return
				default:
					log.Error("Got error while waiting for master, will retry in 5s: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
			} else {
				// this branch should not be executed normally, but network jitter.
				log.Info("Last time I'm the gm leader, and this time I'm the gm leader too.")
				time.Sleep(300 * time.Second)
			}
		}
	}()

	go func() {
		for {
			cmID, err := masterParticipation.GetCurrentMasterID(ctx)
			if err != nil {
				log.Error("fail to get current master ID. err:[%v]", err)
				time.Sleep(5 * time.Second)
				continue
			}
			if cmID != "" && cmID != gm.config.ClusterCfg.GmNodeIp+":"+gm.config.ClusterCfg.GmNodeId {
				gm.currentGMLeaderNodeID = cmID
				gm.currentGMLeaderAddr = strings.Split(cmID, ":")[0]
			}
			time.Sleep(60 * time.Second)
		}
	}()
}
