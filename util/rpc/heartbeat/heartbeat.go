package heartbeat

import (
	"context"

	"github.com/tiglabs/baudengine/util/log"
)

// HeartbeatService exposes a method to echo its request params.
type HeartbeatService struct {
	ClusterID string
}

// Ping echos the contents of the request to the response.
func (hs *HeartbeatService) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	pong := req.Ping
	// Check cluster id
	if req.ClusterId != hs.ClusterID {
		log.Error("client cluster_id(%s) doesn't match server cluster_id(%s)", req.ClusterId, hs.ClusterID)
		pong = "reject"
	}

	return &PingResponse{
		Pong:      pong,
		ClusterId: hs.ClusterID,
	}, nil
}
