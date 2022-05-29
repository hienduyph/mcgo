// Manage a connection pool to a list of servers

package mcgo

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/hienduyph/goss/logger"
)

func NewPool(ctx context.Context, addrs []string, opts ...poolOpt) (*Pool, error) {
	cfg := defaultPoolConfig()
	for _, fn := range opts {
		fn(cfg)
	}
	pool := &Pool{
		cfg:            cfg,
		addrs:          addrs,
		freeConns:      make(map[string][]*Conn, len(addrs)),
		usedConns:      make(map[string]map[*Conn]struct{}, len(addrs)),
		freeConnStream: make(map[string]chan struct{}, len(addrs)),
	}
	for _, addr := range addrs {
		pool.freeConns[addr] = make([]*Conn, 0, cfg.MaxConnPerNode)
		pool.usedConns[addr] = make(map[*Conn]struct{}, cfg.MaxConnPerNode)
		pool.freeConnStream[addr] = make(chan struct{}, cfg.MaxConnPerNode)
	}
	// now try to connect to each addrs
	return pool, nil
}

type Conn struct {
	Addr string
	rw   *bufio.ReadWriter
	// the underling connection
	cnn net.Conn
	// the pool that manages the conn, use for free the connection
	pool *Pool
}

// Release put the connection back the pool for reuse
func (c *Conn) Release() error {
	return c.pool.Release(c)
}

func (c *Conn) Close() error {
	c.rw.Flush()
	return c.cnn.Close()
}

type Pool struct {
	// list of address to connnect
	addrs []string
	// think about conn manager, implement reconnect mechanism
	freeConns      map[string][]*Conn
	freeConnStream map[string]chan struct{}
	usedConns      map[string]map[*Conn]struct{}
	lock           sync.Mutex
	cfg            *poolconfig
}

func (n *Pool) Get(ctx context.Context, addr string) (cn *Conn, err error) {
	n.lock.Lock()
	stream := n.freeConnStream[addr]
	cnns, ok := n.freeConns[addr]
	if ok && len(cnns) > 0 {
		cn, cnns = cnns[0], cnns[1:]
		n.freeConns[addr] = cnns
		n.usedConns[addr][cn] = struct{}{}
		n.lock.Unlock()
		return
	}

	// check limit of used conns first
	if len(n.usedConns[addr]) < int(n.cfg.MaxConnPerNode) {
		// there's no more connection, so create onces
		cn, err = connect(n, addr)
		if err == nil {
			n.usedConns[addr][cn] = struct{}{}
		}
		n.lock.Unlock()
		return
	}
	n.lock.Unlock()

	logger.Debug("reach the max limits, waiting for new conn", "addr", addr)

	// reach the limit, now wait for free conn comings
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case _ = <-stream:
		logger.Debug("Receives new free conn. Try to connect again", "addr", addr)
		// hmm, should we setup try send methods ?
		return n.Get(ctx, addr)
	}
}

func (n *Pool) Release(cnn *Conn) error {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.freeConns[cnn.Addr] = append(n.freeConns[cnn.Addr], cnn)
	delete(n.usedConns[cnn.Addr], cnn)
	n.freeConnStream[cnn.Addr] <- struct{}{}
	logger.Debug("Push connection back to pool", "addr", cnn.Addr, "pool size", len(n.freeConns[cnn.Addr]))
	return nil
}

func (n *Pool) Close() error {
	n.lock.Lock()
	defer n.lock.Unlock()
	for _, cnns := range n.freeConns {
		for _, cn := range cnns {
			cn.Close()
		}
	}
	for _, cnns := range n.usedConns {
		for cn := range cnns {
			cn.Close()
		}
	}
	return nil

}

func connect(n *Pool, addr string) (*Conn, error) {
	logger.Debug("Create new connection", "addr", addr)
	tcp, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}
	cnn := &Conn{
		Addr: addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(tcp), bufio.NewWriter(tcp)),
		cnn:  tcp,
		pool: n,
	}
	return cnn, nil
}

type poolOpt func(*poolconfig)

func WithOptMaxConnPerNode(num uint32) poolOpt {
	return func(p *poolconfig) {
		if num > 0 {
			p.MaxConnPerNode = num
		} else {
			logger.Info("Try to set num is zero")
		}
	}
}

func defaultPoolConfig() *poolconfig {
	return &poolconfig{
		MaxConnPerNode: 10,
	}
}

type poolconfig struct {
	MaxConnPerNode uint32
}

func shard(key string) {
	// shard the key, mod the clients
	// _ = len(n.addrs) % int(crc32.ChecksumIEEE([]byte(key)))
}
