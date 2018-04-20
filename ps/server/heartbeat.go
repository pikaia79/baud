package server

import (
	"time"

	"github.com/tiglabs/baud/proto/masterpb"
	"github.com/tiglabs/baud/proto/metapb"
	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/uuid"
)

type heartbeatWork struct {
	server         *Server
	tickerInterval time.Duration
	minInterval    time.Duration
	lastHeartbeat  time.Time
	nextHeartbeat  time.Time
	ticker         *time.Timer
	triggerChan    chan *struct{}
}

func newHeartbeatWork(server *Server) *heartbeatWork {
	return &heartbeatWork{
		server:         server,
		minInterval:    10 * time.Millisecond,
		tickerInterval: time.Millisecond * time.Duration(server.config.HeartbeatInterval),
		ticker:         new(time.Timer),
		triggerChan:    make(chan *struct{}, 1),
	}
}

func (h *heartbeatWork) trigger() {
	select {
	case h.triggerChan <- nil:
	default:
		return
	}
}

func (h *heartbeatWork) run() {
	h.ticker.Reset(h.tickerInterval)
	h.nextHeartbeat = time.Now().Add(h.tickerInterval)

	for {
		select {
		case <-h.server.context.Done():
			h.ticker.Stop()
			return

		case <-h.ticker.C:
			h.doHeartbeat()
			h.reset()

		case <-h.triggerChan:
			now := time.Now()
			if h.nextHeartbeat.After(now) && h.nextHeartbeat.Sub(now) <= h.minInterval {
				break
			}

			if !h.lastHeartbeat.IsZero() && h.lastHeartbeat.Before(now) && now.Sub(h.lastHeartbeat) <= h.minInterval {
				h.ticker.Reset(h.minInterval)
				h.nextHeartbeat = time.Now().Add(h.minInterval)
				break
			}

			h.doHeartbeat()
			h.reset()
		}
	}
}

func (h *heartbeatWork) reset() {
	h.ticker.Reset(h.tickerInterval)
	now := time.Now()
	h.lastHeartbeat = now
	h.nextHeartbeat = now.Add(h.tickerInterval)
}

func (h *heartbeatWork) doHeartbeat() {
	req := &masterpb.PSHeartbeatRequest{
		RequestHeader: metapb.RequestHeader{ReqId: uuid.FlakeUUID()},
		NodeID:        h.server.nodeID,
		Partitions:    make([]masterpb.PartitionInfo, 0),
	}
	stats, _ := h.server.systemMetric.Export()

	h.server.partitions.Range(func(key, value interface{}) bool {
		pinfo := value.(*partition).getPartitionInfo()
		req.Partitions = append(req.Partitions, *pinfo)
		stats.Ops += pinfo.Statistics.Ops
		return true
	})
	req.SysStats = *stats

	masterClient, _ := h.server.masterClient.GetGrpcClient(h.server.config.MasterServer)
	resp, err := masterClient.(masterpb.MasterRpcClient).PSHeartbeat(h.server.context, req)
	if err != nil {
		log.Error("heartbeat failed error: %v", err)
		return
	}

	if resp.Code == metapb.MASTER_RESP_CODE_HEARTBEAT_REGISTRY {
		log.Error("heartbeat response reset,then server stating cleanning...")
		h.server.resgitry()
	}
}
