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
	option ClientOption

	rwMutex   sync.RWMutex
	conn      *connection
	clientRaw interface{}
}

func (cw *clientWrapper) getClient() (interface{}, error) {
	cw.rwMutex.RLock()
	if cw.conn != nil {
		if _, err := cw.conn.connect(); err == nil {
			cw.rwMutex.RUnlock()

			return cw.clientRaw, nil
		}
	}
	cw.rwMutex.RUnlock()

	cw.rwMutex.Lock()
	if cw.conn != nil {
		if _, err := cw.conn.connect(); err == nil {
			cw.rwMutex.Unlock()
			return cw.clientRaw, nil
		}
		cw.option.ConnectMgr.removeConn(cw.key, cw.conn)
	}

	var cli interface{}
	cw.conn = cw.option.ConnectMgr.grpcDial(cw.key, cw.addr, &cw.option)
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

type loadPoll struct {
	pos  uint64
	mask uint64
	next func() uint64
}

type clientPool struct {
	size     uint32
	loadPoll *loadPoll
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
		pool.wrappers[i].option = *option
	}

	if size > 1 {
		if size&(size-1) == 0 {
			pool.loadPoll = &loadPoll{mask: uint64(size - 1)}
			pool.loadPoll.next = func() uint64 {
				pos := atomic.AddUint64(&pool.loadPoll.pos, 1)
				return pos & pool.loadPoll.mask
			}
		} else {
			pool.loadPoll = &loadPoll{mask: uint64(size)}
			pool.loadPoll.next = func() uint64 {
				pos := atomic.AddUint64(&pool.loadPoll.pos, 1)
				return pos % pool.loadPoll.mask
			}
		}
	}

	return pool
}

func (p *clientPool) getClient() (interface{}, error) {
	if p.size == 1 {
		return p.wrappers[0].getClient()
	}

	idx := p.loadPoll.next()
	return p.wrappers[idx].getClient()
}

func (p *clientPool) Close() error {
	for i := 0; i < len(p.wrappers); i++ {
		p.wrappers[i].Close()
	}
	return nil
}
