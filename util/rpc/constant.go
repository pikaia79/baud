package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/transport"

	"github.com/tiglabs/baudengine/util/multierror"
)

const (
	defaultWindowSize            = 65535
	defaultInitialWindowSize     = defaultWindowSize * 32
	defaultInitialConnWindowSize = defaultInitialWindowSize * 16

	defaultConcurrentStreams  = math.MaxInt32
	defaultRecvMsgSize        = math.MaxInt32
	defaultSendMsgSize        = math.MaxInt32
	defaultMaxCallRecvMsgSize = math.MaxInt32
	defaultMaxCallSendMsgSize = math.MaxInt32

	defaultConnectTimeout    = 3 * time.Second
	defaultKeepaliveTime     = 30 * time.Minute
	defaultKeepaliveTimeout  = time.Minute
	defaultHeartbeatInterval = 15 * time.Minute
	defaultHeartbeatTimeout  = 3 * time.Second
	defaultMaxBackoff        = 30 * time.Millisecond

	defaultCompression = false
)

var (
	// ErrNotHeartbeated connection not heartbeat error
	ErrNotHeartbeated = errors.New("grpc connection not yet heartbeated")
	// ErrorUnavailable server unavailable error
	ErrorUnavailable = errors.New("the server is unavailable")
	// ErrCannotReuseClientConn is returned when a failed connection is being reused.
	ErrCannotReuseClientConn = errors.New("cannot reuse client connection")
)

func init() {
	// Disable GRPC tracing entirely, which is very expensive for snapshots.
	grpc.EnableTracing = false
	encoding.RegisterCompressor(snappyCompressor{})
}

// IsClosedGrpcConnection returns true if err's Cause is an error produced by gRPC on closed connections.
func IsClosedGrpcConnection(err error) bool {
	err = multierror.Cause(err)
	if err == ErrCannotReuseClientConn {
		return true
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Canceled ||
			s.Code() == codes.Unavailable ||
			s.Message() == grpc.ErrClientConnClosing.Error() {
			return true
		}
	}
	if err == context.Canceled || err == grpc.ErrServerStopped || err == io.EOF ||
		strings.Contains(err.Error(), "is closing") ||
		strings.Contains(err.Error(), "tls: use of closed connection") ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), io.ErrClosedPipe.Error()) ||
		strings.Contains(err.Error(), io.EOF.Error()) ||
		strings.Contains(err.Error(), "node unavailable") {
		return true
	}
	if streamErr, ok := err.(transport.StreamError); ok && streamErr.Code == codes.Canceled {
		return true
	}

	return false
}

// IsConnectionReady returns nil if the given connection is ready to send a request.
func IsConnectionReady(conn *grpc.ClientConn) error {
	if s := conn.GetState(); s == connectivity.TransientFailure {
		return fmt.Errorf("connection not ready: %s", s)
	}
	return nil
}
