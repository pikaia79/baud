package rpc

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/routine"
	"github.com/tiglabs/baud/util/rpc/heartbeat"
)

// DefaultManagerOption create a default option
var DefaultManagerOption = ManagerOption{
	HeartbeatInterval: defaultHeartbeatInterval,
	HeartbeatTimeout:  defaultHeartbeatTimeout,
}

// ManagerOption contains the fields required by the rpc framework.
type ManagerOption struct {
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	HeartbeatCallback func()
}

// ConnectionMgr grpc connection manager.
type ConnectionMgr struct {
	ctx               context.Context
	cancelFunc        context.CancelFunc
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	heartbeatCallback func()

	conns *sync.Map
}

// NewConnectionMgr creates an connection manager with the supplied values.
func NewConnectionMgr(ctx context.Context, option *ManagerOption) *ConnectionMgr {
	mgr := &ConnectionMgr{
		heartbeatInterval: option.HeartbeatInterval,
		heartbeatTimeout:  option.HeartbeatTimeout,
		heartbeatCallback: option.HeartbeatCallback,
		conns:             new(sync.Map),
	}
	mgr.ctx, mgr.cancelFunc = context.WithCancel(ctx)
	routine.AddCancel(func() {
		mgr.Close()
	})

	return mgr
}

// grpcDial connects to the address
func (mgr *ConnectionMgr) grpcDial(key, target string, option *ClientOption) *connection {
	value, ok := mgr.conns.Load(key)
	if !ok {
		value, _ = mgr.conns.LoadOrStore(key, newConnection(mgr.ctx, option.ClusterID))
	}

	conn := value.(*connection)
	conn.initOnce.Do(func() {
		var redialChan <-chan struct{}
		conn.grpcConn, redialChan, conn.dialErr = mgr.grpcDialRaw(target, option)
		if conn.dialErr == nil {
			routine.RunWorkAsync("GRPC-HEARTBEAT", func() {
				err := mgr.runHeartbeat(conn, target, redialChan)
				if err != nil && !IsClosedGrpcConnection(err) {
					log.Error("removing connection to %s due to error: %s", target, err)
				}
				mgr.removeConn(key, conn)
			}, routine.LogPanic(false))
		}
	})

	return conn
}

func (mgr *ConnectionMgr) grpcDialRaw(target string, option *ClientOption) (*grpc.ClientConn, <-chan struct{}, error) {
	dialOpts := make([]grpc.DialOption, 0, 16)

	dialOpts = append(dialOpts, grpc.WithInsecure())
	dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(option.MaxCallRecvMsgSize),
		grpc.MaxCallSendMsgSize(option.MaxCallSendMsgSize),
	))
	dialOpts = append(dialOpts, grpc.WithBackoffMaxDelay(option.MaxBackoff))
	dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: time.Duration(math.MaxInt64)}))
	dialOpts = append(dialOpts,
		grpc.WithInitialWindowSize(option.InitialWindowSize),
		grpc.WithInitialConnWindowSize(option.InitialConnWindowSize))

	if option.Compression {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(grpc.UseCompressor((snappyCompressor{}).Name())))
	}
	if option.StatsHandler != nil {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(option.StatsHandler))
	}

	dialer := onceDialer{
		redialChan: make(chan struct{}),
	}
	dialOpts = append(dialOpts, grpc.WithDialer(dialer.dial))

	connCtx, _ := context.WithTimeout(mgr.ctx, option.ConnectTimeout)
	conn, err := grpc.DialContext(connCtx, target, dialOpts...)
	return conn, dialer.redialChan, err
}

func (mgr *ConnectionMgr) runHeartbeat(conn *connection, target string, redialChan <-chan struct{}) error {
	request := heartbeat.PingRequest{
		Ping:      "OK",
		ClusterId: conn.clusterID,
	}
	heartbeatClient := heartbeat.NewHeartbeatClient(conn.grpcConn)

	var heartbeatTimer time.Timer
	defer heartbeatTimer.Stop()

	heartbeatTimer.Reset(0)
	succeeded := false
	for {
		select {
		case <-redialChan:
			conn.heartbeatResult.Store(heartbeatResult{
				succeeded: succeeded,
				err:       ErrCannotReuseClientConn,
			})
			conn.setInitialHeartbeatDone()
			return ErrCannotReuseClientConn

		case <-conn.ctx.Done():
			conn.heartbeatResult.Store(heartbeatResult{
				succeeded: succeeded,
				err:       conn.ctx.Err(),
			})
			conn.setInitialHeartbeatDone()
			return conn.ctx.Err()

		case <-heartbeatTimer.C:
		}

		goCtx := conn.ctx
		var cancel context.CancelFunc
		if hbTimeout := mgr.heartbeatTimeout; hbTimeout > 0 {
			goCtx, cancel = context.WithTimeout(goCtx, hbTimeout)
		}
		response, err := heartbeatClient.Ping(goCtx, &request)
		if cancel != nil {
			cancel()
		}
		if err == nil {
			if request.ClusterId != response.ClusterId {
				err = fmt.Errorf("client cluster_id(%s) doesn't match server cluster_id(%s)", request.ClusterId, response.ClusterId)
			}
		}

		if err == nil {
			succeeded = true
			if cb := mgr.heartbeatCallback; cb != nil {
				cb()
			}
		}
		conn.heartbeatResult.Store(heartbeatResult{
			succeeded: succeeded,
			err:       err,
		})
		conn.setInitialHeartbeatDone()

		heartbeatTimer.Reset(mgr.heartbeatInterval)
	}
}

// Close close ConnectionMgr
func (mgr *ConnectionMgr) Close() error {
	mgr.cancelFunc()
	mgr.conns.Range(func(k, v interface{}) bool {
		conn := v.(*connection)
		conn.initOnce.Do(func() {
			if conn.dialErr == nil {
				conn.dialErr = ErrorUnavailable
			}
		})
		mgr.removeConn(k.(string), conn)
		return true
	})

	return nil
}

func (mgr *ConnectionMgr) removeConn(key string, conn *connection) {
	mgr.conns.Delete(key)
	conn.Close()
}
