package rpc

import (
	"context"
	"math"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
)

const (
	defaultConcurrentStreams = math.MaxInt32
	defaultRecvMsgSize       = math.MaxInt32
	defaultSendMsgSize       = math.MaxInt32
	defaultWindowSize        = 65535 * 32
	defaultConnWindowSize    = defaultWindowSize * 16
	defaultKeepaliveTime     = 3 * time.Second
	defaultKeepaliveTimeout  = 3 * time.Second
)

// By default, gRPC disconnects clients that send "too many" pings,
// we don't really care about that, so configure the server to be as permissive as possible.
var serverEnforcement = keepalive.EnforcementPolicy{
	MinTime:             time.Nanosecond,
	PermitWithoutStream: true,
}

// Option contains the fields required by the rpc framework.
type Option struct {
	MaxRecvMsgSize        int
	MaxSendMsgSize        int
	InitialWindowSize     int32
	InitialConnWindowSize int32
	MaxConcurrentStreams  uint32
	KeepaliveTime         time.Duration
	KeepaliveTimeout      time.Duration
	StatsHandler          stats.Handler
	Interceptor           func(fullMethod string) error
}

// DefaultOption create a default option
func DefaultOption() *Option {
	return &Option{
		MaxRecvMsgSize:        defaultRecvMsgSize,
		MaxSendMsgSize:        defaultSendMsgSize,
		InitialWindowSize:     defaultWindowSize,
		InitialConnWindowSize: defaultConnWindowSize,
		MaxConcurrentStreams:  defaultConcurrentStreams,
		KeepaliveTime:         defaultKeepaliveTime,
		KeepaliveTimeout:      defaultKeepaliveTimeout,
	}
}

// CreateGrpcServer is create a grpc server instance.
func CreateGrpcServer(option *Option) *grpc.Server {
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

	return grpc.NewServer(opts...)
}
