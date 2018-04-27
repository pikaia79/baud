package rpc

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"

	"github.com/tiglabs/baudengine/util/rpc/heartbeat"
)

// By default, gRPC disconnects clients that send "too many" pings,
// We configure the server to be as permissive as possible.
var serverEnforcement = keepalive.EnforcementPolicy{
	MinTime:             time.Nanosecond,
	PermitWithoutStream: true,
}

// DefaultServerOption create a default option
var DefaultServerOption = ServerOption{
	MaxRecvMsgSize:        defaultRecvMsgSize,
	MaxSendMsgSize:        defaultSendMsgSize,
	InitialWindowSize:     defaultInitialWindowSize,
	InitialConnWindowSize: defaultInitialConnWindowSize,
	MaxConcurrentStreams:  defaultConcurrentStreams,
	KeepaliveTime:         defaultKeepaliveTime,
	KeepaliveTimeout:      defaultKeepaliveTimeout,
}

// ServerOption contains the fields required by the rpc framework.
type ServerOption struct {
	MaxRecvMsgSize        int
	MaxSendMsgSize        int
	InitialWindowSize     int32
	InitialConnWindowSize int32
	MaxConcurrentStreams  uint32
	KeepaliveTime         time.Duration
	KeepaliveTimeout      time.Duration

	ClusterID    string
	StatsHandler stats.Handler
	Interceptor  func(fullMethod string) error
}

// NewGrpcServer is create a grpc server instance.
func NewGrpcServer(option *ServerOption) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(option.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(option.MaxSendMsgSize),
		grpc.InitialWindowSize(option.InitialWindowSize),
		grpc.InitialConnWindowSize(option.InitialConnWindowSize),
		grpc.MaxConcurrentStreams(option.MaxConcurrentStreams),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: option.KeepaliveTime, Timeout: option.KeepaliveTimeout}),
		grpc.KeepaliveEnforcementPolicy(serverEnforcement),
	}
	if option.StatsHandler != nil {
		opts = append(opts, grpc.StatsHandler(option.StatsHandler))
	}

	var unaryInterceptor grpc.UnaryServerInterceptor
	var streamInterceptor grpc.StreamServerInterceptor
	if option.Interceptor != nil {
		unaryInterceptor = func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			if err := option.Interceptor(info.FullMethod); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}

		streamInterceptor = func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := option.Interceptor(info.FullMethod); err != nil {
				return err
			}
			return handler(srv, stream)
		}
	}
	if unaryInterceptor != nil {
		opts = append(opts, grpc.UnaryInterceptor(unaryInterceptor))
	}
	if streamInterceptor != nil {
		opts = append(opts, grpc.StreamInterceptor(streamInterceptor))
	}

	server := grpc.NewServer(opts...)
	heartbeat.RegisterHeartbeatServer(server, &heartbeat.HeartbeatService{ClusterID: option.ClusterID})
	return server
}
