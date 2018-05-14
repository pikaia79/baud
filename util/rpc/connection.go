package rpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/rpc/heartbeat"
)

type heartbeatResult struct {
	succeeded bool
	err       error
}

// connection is a wrapper around grpc.ClientConn
type connection struct {
	addr            string
	heartbeatReq    heartbeat.PingRequest
	manager         *ConnectionMgr
	grpcConn        *grpc.ClientConn
	dialErr         error        // error while dialing; if set, connection is unusable
	heartbeatResult atomic.Value // result of latest heartbeat
	redialChan      <-chan struct{}

	initOnce      sync.Once
	validatedOnce sync.Once
	closeOnce     sync.Once
	heartbeatDone chan struct{} // closed after first heartbeat
	closeDone     chan struct{}
}

func newConnection(clusterID, addr string, manager *ConnectionMgr) *connection {
	c := &connection{
		addr:          addr,
		heartbeatReq:  heartbeat.PingRequest{Ping: "OK", ClusterId: clusterID},
		manager:       manager,
		heartbeatDone: make(chan struct{}),
		closeDone:     make(chan struct{}),
	}

	c.heartbeatResult.Store(heartbeatResult{
		succeeded: false,
		err:       ErrNotHeartbeated,
	})
	return c
}

func (c *connection) connect() (*grpc.ClientConn, error) {
	select {
	case <-c.closeDone:
		if c.dialErr == nil {
			c.dialErr = ErrCannotReuseClientConn
		}
	default:
	}

	if c.dialErr != nil {
		return nil, c.dialErr
	}

	// Wait for initial heartbeat
	select {
	case <-c.heartbeatDone:
	case <-c.manager.ctx.Done():
		return nil, c.manager.ctx.Err()
	}

	h := c.heartbeatResult.Load().(heartbeatResult)
	if !h.succeeded {
		return nil, fmt.Errorf("initial connection heartbeat failed: %v", h.err)
	}

	if err := IsConnectionReady(c.grpcConn); err != nil {
		return nil, err
	}
	return c.grpcConn, nil
}

func (c *connection) setInitialHeartbeatDone() {
	c.validatedOnce.Do(func() {
		close(c.heartbeatDone)
	})
}

func (c *connection) heartbeat() error {
	var err error
	var response *heartbeat.PingResponse
	success := c.heartbeatResult.Load().(heartbeatResult).succeeded

	select {
	case <-c.redialChan:
		err = ErrCannotReuseClientConn

	case <-c.manager.ctx.Done():
		err = c.manager.ctx.Err()

	default:
		goCtx := c.manager.ctx
		var cancel context.CancelFunc
		if hbTimeout := c.manager.option.HeartbeatTimeout; hbTimeout > 0 {
			goCtx, cancel = context.WithTimeout(goCtx, hbTimeout)
		}
		heartbeatClient := heartbeat.NewHeartbeatClient(c.grpcConn)
		response, err = heartbeatClient.Ping(goCtx, &c.heartbeatReq)
		if cancel != nil {
			cancel()
		}

		if err == nil {
			if c.heartbeatReq.ClusterId != response.ClusterId {
				err = fmt.Errorf("client cluster_id(%s) doesn't match server cluster_id(%s)", c.heartbeatReq.ClusterId, response.ClusterId)
			}
		}
		if err == nil {
			success = true
			if cb := c.manager.option.HeartbeatCallback; cb != nil {
				cb()
			}
		}
	}

	c.heartbeatResult.Store(heartbeatResult{
		succeeded: success,
		err:       err,
	})
	c.setInitialHeartbeatDone()

	return err
}

func (c *connection) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeDone)

		if grpcConn := c.grpcConn; grpcConn != nil {
			if err := grpcConn.Close(); err != nil && !IsClosedGrpcConnection(err) {
				log.Error("failed to close client connection: %s", err)
			}
		}
	})
	return nil
}

// onceDialer implements the grpc.WithDialer interface. This ensures that initial heartbeat occurs on every new connection.
type onceDialer struct {
	sync.Mutex
	dialed     bool
	closed     bool
	redialChan chan struct{}
}

func (d *onceDialer) dial(addr string, timeout time.Duration) (net.Conn, error) {
	d.Lock()

	if !d.dialed {
		d.dialed = true
		dialer := net.Dialer{
			Timeout: timeout,
		}
		d.Unlock()
		return dialer.Dial("tcp", addr)
	} else if !d.closed {
		d.closed = true
		close(d.redialChan)
	}

	d.Unlock()
	return nil, ErrCannotReuseClientConn
}
