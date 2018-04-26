package rpc

import (
	"context"
	"math"
	"runtime"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/tiglabs/baud/util/log"
	"github.com/tiglabs/baud/util/routine"
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
	ctx        context.Context
	cancelFunc context.CancelFunc
	option     ManagerOption

	conns sync.Map
}

// NewConnectionMgr creates an connection manager with the supplied values.
func NewConnectionMgr(ctx context.Context, option *ManagerOption) *ConnectionMgr {
	mgr := &ConnectionMgr{
		option: *option,
	}
	mgr.ctx, mgr.cancelFunc = context.WithCancel(ctx)
	routine.AddCancel(func() {
		mgr.Close()
	})
	routine.RunWorkDaemon("GRPC-HEARTBEAT", mgr.runHeartbeat, mgr.ctx.Done())

	return mgr
}

// grpcDial connects to the address
func (mgr *ConnectionMgr) grpcDial(key, target string, option *ClientOption) *connection {
	value, ok := mgr.conns.Load(key)
	if !ok {
		value, _ = mgr.conns.LoadOrStore(key, newConnection(option.ClusterID, target, mgr))
	}

	conn := value.(*connection)
	conn.initOnce.Do(func() {
		conn.grpcConn, conn.redialChan, conn.dialErr = mgr.grpcDialRaw(target, option)
		if conn.dialErr == nil {
			routine.RunWork("GRPC-DOINITHEARTBEAT", func() error {
				err := conn.heartbeat()
				if err != nil && !IsClosedGrpcConnection(err) {
					log.Error("removing connection to %s due to error: %s", target, err)
				}
				mgr.removeConn(key, conn)
				return err
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

func (mgr *ConnectionMgr) runHeartbeat() {
	heartbeatTimer := time.NewTimer(mgr.option.HeartbeatInterval)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-mgr.ctx.Done():
			return

		case <-heartbeatTimer.C:
			count := 0
			mgr.conns.Range(func(k, v interface{}) bool {
				conn := v.(*connection)
				routine.RunWorkAsync("GRPC-DOHEARTBEAT", func() {
					select {
					case <-conn.closeDone:
						return
					default:
					}

					select {
					case <-conn.manager.ctx.Done():
						return

					case <-conn.heartbeatDone:
						err := conn.heartbeat()
						if err != nil && !IsClosedGrpcConnection(err) {
							log.Error("grpc heartbeat failed to %s due to error: %s", conn.addr, err)
						}

					default:
					}
				}, routine.LogPanic(false))

				count++
				if (count & 63) == 0 {
					runtime.Gosched()
				}
				return true
			})
		}

		heartbeatTimer.Reset(mgr.option.HeartbeatInterval)
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
