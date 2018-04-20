package rpc

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

type createClientFunc func(cc *grpc.ClientConn) interface{}

// DefaultClientOption create a default option
var DefaultClientOption = ClientOption{
	Compression:           defaultCompression,
	ConnectTimeout:        defaultConnectTimeout,
	InitialWindowSize:     defaultInitialWindowSize,
	InitialConnWindowSize: defaultInitialConnWindowSize,
	MaxCallRecvMsgSize:    defaultMaxCallRecvMsgSize,
	MaxCallSendMsgSize:    defaultMaxCallSendMsgSize,
	MaxBackoff:            defaultMaxBackoff,
}

// ClientOption  contains the fields required by the rpc framework.
type ClientOption struct {
	Compression           bool
	InitialWindowSize     int32
	InitialConnWindowSize int32
	MaxCallRecvMsgSize    int
	MaxCallSendMsgSize    int
	MaxBackoff            time.Duration
	ConnectTimeout        time.Duration

	ClusterID    string
	ConnectMgr   *ConnectionMgr
	CreateFunc   createClientFunc
	StatsHandler stats.Handler
}

// Client grpc wrap client
type Client struct {
	concurrent uint32
	option     ClientOption
	pools      sync.Map
}

// NewClient create a Client object
func NewClient(concurrent uint32, option *ClientOption) *Client {
	return &Client{
		concurrent: concurrent,
		option:     *option,
	}
}

// GetGrpcClient return grpc client
func (c *Client) GetGrpcClient(addr string) (interface{}, error) {
	value, ok := c.pools.Load(addr)
	if !ok {
		value, _ = c.pools.LoadOrStore(addr, newClientPool(c.concurrent, addr, &c.option))
	}

	return value.(*clientPool).getClient()
}

// Close close client
func (c *Client) Close() error {
	c.pools.Range(func(k, v interface{}) bool {
		v.(*clientPool).Close()
		c.pools.Delete(k)
		return true
	})

	return nil
}

type clientWrapper struct {
	key    string
	addr   string
	option *ClientOption

	rwMutex   sync.RWMutex
	conn      *connection
	clientRaw interface{}
}

func (cw *clientWrapper) getClient() (interface{}, error) {
	cw.rwMutex.RLock()
	if cw.conn != nil {
		if _, err := cw.conn.connect(); err == nil {
			return cw.clientRaw, nil
		}
	}
	cw.rwMutex.RUnlock()

	cw.rwMutex.Lock()
	if cw.conn != nil {
		if _, err := cw.conn.connect(); err == nil {
			return cw.clientRaw, nil
		}
		cw.option.ConnectMgr.removeConn(cw.key, cw.conn)
	}

	var cli interface{}
	cw.conn = cw.option.ConnectMgr.grpcDial(cw.key, cw.addr, cw.option)
	grpcConn, err := cw.conn.connect()
	if err == nil {
		cli = cw.option.CreateFunc(grpcConn)
	} else {
		cw.option.ConnectMgr.removeConn(cw.key, cw.conn)
		cw.conn = nil
	}
	cw.clientRaw = cli
	cw.rwMutex.Unlock()

	return cli, err
}

func (cw *clientWrapper) Close() error {
	cw.rwMutex.Lock()
	if cw.conn != nil {
		cw.option.ConnectMgr.removeConn(cw.key, cw.conn)
		cw.conn = nil
	}
	cw.rwMutex.Unlock()

	return nil
}

type clientPool struct {
	size     uint32
	pos      uint32
	wrappers []clientWrapper
}

func newClientPool(size uint32, addr string, option *ClientOption) *clientPool {
	pool := &clientPool{
		size:     size,
		wrappers: make([]clientWrapper, size),
	}

	for i := 0; i < int(size); i++ {
		pool.wrappers[i].key = strings.Join([]string{addr, strconv.Itoa(i)}, "-")
		pool.wrappers[i].addr = addr
		pool.wrappers[i].option = option
	}

	return pool
}

func (p *clientPool) getClient() (interface{}, error) {
	idx := atomic.AddUint32(&p.pos, 1)
	if idx >= p.size {
		atomic.StoreUint32(&p.pos, 0)
		idx = 0
	}

	return p.wrappers[idx].getClient()
}

func (p *clientPool) Close() error {
	for i := 0; i < len(p.wrappers); i++ {
		p.wrappers[i].Close()
	}
	return nil
}
