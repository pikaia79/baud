package server

import (
	"context"
	"time"

	"github.com/tiglabs/baudengine/proto/masterpb"
	"github.com/tiglabs/baudengine/proto/metapb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/routine"
	"github.com/tiglabs/baudengine/util/uuid"
)

const (
	heartbeatTimeout = 10 * time.Second
)

type heartbeatWork struct {
	server         *Server
	tickerInterval time.Duration
	minInterval    time.Duration
	lastHeartbeat  time.Time
	nextHeartbeat  time.Time
	triggerCh      chan *struct{}
	stopCh         chan *struct{}
}

func newHeartbeatWork(server *Server) *heartbeatWork {
	return &heartbeatWork{
		server:         server,
		minInterval:    10 * time.Millisecond,
		tickerInterval: time.Millisecond * time.Duration(server.HeartbeatInterval),
		triggerCh:      make(chan *struct{}, 1),
		stopCh:         make(chan *struct{}),
	}
}

func (h *heartbeatWork) start() {
	quitCh := make(chan struct{})
	routine.RunWorkDaemon("MASTER-HEARTBEAT", func() {
		heartbeatTimer := time.NewTimer(h.tickerInterval)
		defer heartbeatTimer.Stop()

		h.update(heartbeatTimer)
		h.lastHeartbeat = time.Time{}
		for {
			select {
			case <-h.server.ctx.Done():
				close(quitCh)
				return

			case <-h.stopCh:
				close(quitCh)
				return

			case <-heartbeatTimer.C:
				h.doHeartbeat()
				h.update(&heartbeatTimer)

			case <-h.triggerCh:
				now := time.Now()
				if h.nextHeartbeat.After(now) && h.nextHeartbeat.Sub(now) <= h.minInterval {
					break
				}

				if !h.lastHeartbeat.IsZero() && h.lastHeartbeat.Before(now) && now.Sub(h.lastHeartbeat) <= h.minInterval {
					heartbeatTimer.Reset(h.minInterval)
					h.nextHeartbeat = time.Now().Add(h.minInterval)
					break
				}

				h.doHeartbeat()
				h.update(&heartbeatTimer)
			}
		}
	}, quitCh)
}

func (h *heartbeatWork) stop() {
	h.stopCh <- nil
}

func (h *heartbeatWork) trigger() {
	select {
	case h.triggerCh <- nil:
	default:
		return
	}
}

func (h *heartbeatWork) update(timer *time.Timer) {
	now := time.Now()
	timer.Reset(h.tickerInterval)
	h.lastHeartbeat = now
	h.nextHeartbeat = now.Add(h.tickerInterval)
}

func (h *heartbeatWork) doHeartbeat() {
	masterClient, err := h.server.masterClient.GetGrpcClient(h.server.MasterServer)
	if err != nil {
		log.Error("get master heartbeat rpc client error: %v", err)
		return
	}

	stats, _ := h.server.systemMetric.Export()
	req := &masterpb.PSHeartbeatRequest{
		RequestHeader: metapb.RequestHeader{ReqId: uuid.FlakeUUID()},
		NodeID:        h.server.nodeID,
		Partitions:    make([]masterpb.PartitionInfo, 0),
	}
	h.server.partitions.Range(func(key, value interface{}) bool {
		pinfo := value.(*partition).getPartitionInfo()
		req.Partitions = append(req.Partitions, *pinfo)
		stats.Ops += pinfo.Statistics.Ops
		return true
	})
	req.SysStats = *stats

	goCtx, cancel := context.WithTimeout(h.server.ctx, heartbeatTimeout)
	resp, err := masterClient.(masterpb.MasterRpcClient).PSHeartbeat(goCtx, req)
	cancel()

	if err != nil {
		log.Error("master heartbeat request[%s] failed error: %v", req.ReqId, err)
		return
	}

	if resp.Code == metapb.MASTER_RESP_CODE_HEARTBEAT_REGISTRY {
		log.Error("master heartbeat request[%s] ack registry, message is: %s", req.ReqId, resp.Message)
		routine.RunWorkAsync("SERVER-RESTART", func() {
			h.server.restart()
		})
	}
}
