package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/netutil"
)

var sourceAddr = func() net.Addr {
	return &net.TCPAddr{
		IP: netutil.GetPrivateIP(),
	}
}()

type heartbeatResult struct {
	succeeded bool
	err       error
}

// connection is a wrapper around grpc.ClientConn
type connection struct {
	clusterID       string
	ctx             context.Context
	grpcConn        *grpc.ClientConn
	dialErr         error        // error while dialing; if set, connection is unusable
	heartbeatResult atomic.Value // result of latest heartbeat

	initOnce      sync.Once
	validatedOnce sync.Once
	closeOnce     sync.Once
	heartbeatDone chan struct{} // closed after first heartbeat
	closeDone     chan struct{}
}

func newConnection(ctx context.Context, clusterID string) *connection {
	c := &connection{
		ctx:           ctx,
		clusterID:     clusterID,
		heartbeatDone: make(chan struct{}),
		closeDone:     make(chan struct{}),
	}
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
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
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
	defer d.Unlock()

	if !d.dialed {
		d.dialed = true
		dialer := net.Dialer{
			Timeout:   timeout,
			LocalAddr: sourceAddr,
		}
		return dialer.Dial("tcp", addr)
	} else if !d.closed {
		d.closed = true
		close(d.redialChan)
	}
	return nil, ErrCannotReuseClientConn
}
