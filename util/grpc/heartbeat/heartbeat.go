package heartbeat

import (
	"context"
	"fmt"
)

// HeartbeatService exposes a method to echo its request params.
type HeartbeatService struct {
	ClusterID string
}

// Ping echos the contents of the request to the response.
func (hs *HeartbeatService) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	// Check cluster id
	if req.ClusterId != hs.ClusterID {
		return nil, fmt.Errorf("client cluster_id(%s) doesn't match server cluster_id(%s)", req.ClusterId, hs.ClusterID)
	}

	return &PingResponse{
		Pong:      req.Ping,
		ClusterId: hs.ClusterID,
	}, nil
}
