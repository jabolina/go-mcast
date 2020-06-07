package mcast

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/go-msgpack/codec"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

const (
	rpcGMCast uint8 = iota
	rpcCompute
	rpcGather
	rpcRecover
	DefaultTimeoutScale = 256 * 1024 // 256KB
)

var ErrTransportShutdown = errors.New("transport shutdown")

/*

NetworkTransport provides a network based transport that can be
used to communicate on remote machines. It requires
an underlying stream layer to provide a stream abstraction, which can
be simple TCP, TLS, etc.

This transport is very simple and lightweight. Each RPC request is
framed by sending a byte that indicates the message type, followed
by the MsgPack encoded request.

The response is an error string followed by the response object,
both are encoded using MsgPack.

InstallSnapshot is special, in that after the RPC request we stream
the entire state. That socket is not re-used as the connection state
is not known if there is an error.

*/
type NetworkTransport struct {
	connPool     map[ServerAddress][]*netConn
	connPoolLock sync.Mutex

	consumeCh chan RPC

	logger Logger

	maxPool int

	serverAddressResolver ServerAddressResolver

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	stream StreamLayer

	streamCtx     context.Context
	streamCancel  context.CancelFunc
	streamCtxLock sync.RWMutex

	timeout      time.Duration
	TimeoutScale int
}

type NetworkTransportConfig struct {
	// Override the target address when establishing a connection to invoke and RPC.
	ServerAddressResolver ServerAddressResolver

	Logger Logger

	// Dialer.
	Stream StreamLayer

	// How many connections.
	MaxPool int

	// Used for I/O control.
	Timeout time.Duration
}

// Target address to which is invoked an RPC call to establish a connection.
type ServerAddressResolver interface {
	Resolve(id ServerID) (ServerAddress, error)
}

// Context about a network connection
type netConn struct {
	target ServerAddress
	conn   net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	dec    *codec.Decoder
	enc    *codec.Encoder
}

func (n *netConn) Release() error {
	return n.conn.Close()
}

// Creates a new NetworkTransport with the given configuration parameters
func NewNetworkTransportWithConfig(config *NetworkTransportConfig) *NetworkTransport {
	if config.Logger == nil {
		config.Logger = &DefaultLogger{}
	}
	trans := &NetworkTransport{
		connPool:              make(map[ServerAddress][]*netConn),
		consumeCh:             make(chan RPC),
		logger:                config.Logger,
		maxPool:               config.MaxPool,
		shutdownCh:            make(chan struct{}),
		stream:                config.Stream,
		timeout:               config.Timeout,
		TimeoutScale:          DefaultTimeoutScale,
		serverAddressResolver: config.ServerAddressResolver,
	}

	trans.setupStreamContext()
	go trans.listen()

	return trans
}

// Creates a new network transport with the given logger, dialer and listener.
// The maxPool controls how many connections we will pool.
// The timeout is used to apply I/O deadlines.
func NewNetworkTransportWithLogger(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logger Logger,
) *NetworkTransport {
	config := &NetworkTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return NewNetworkTransportWithConfig(config)
}

// Creates a new NetworkTransport with the given parameters.
// is given a dialer and lister
// The maxPool tells how many connections can be polled.
// The timeout parameters used to control log I/O requests.
func NewNetworkTransport(
	stream StreamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *NetworkTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	logger := &DefaultLogger{}
	config := &NetworkTransportConfig{
		Stream:  stream,
		MaxPool: maxPool,
		Timeout: timeout,
		Logger:  logger,
	}
	return NewNetworkTransportWithConfig(config)
}

// Listen for incoming connections.
func (n *NetworkTransport) listen() {
	const baseDelay = 5 * time.Millisecond
	const maxDelay = 1 * time.Second

	var loopDelay time.Duration
	for {
		// Accepts connections
		conn, err := n.stream.Accept()

		// If some error happened readjust the loopDelay
		if err != nil {
			if loopDelay == 0 {
				loopDelay = baseDelay
			} else {
				loopDelay *= 2
			}

			if loopDelay > maxDelay {
				loopDelay = maxDelay
			}

			if !n.IsShutdown() {
				n.logger.Error("failed to accept connect. %v", err)
			}

			// Wait again to proceed
			select {
			case <-n.shutdownCh:
				return
			case <-time.After(loopDelay):
				continue
			}
		}

		loopDelay = 0
		n.logger.Debug("accepted connection with local-address %s and remote-address %s", n.LocalAddress(), conn.RemoteAddr().String())
		go n.handleConn(n.getStreamContext(), conn)
	}
}

// setupStreamContext is used to create a new stream context.
// This should be called with the stream lock held.
func (n *NetworkTransport) setupStreamContext() {
	ctx, cancel := context.WithCancel(context.Background())
	n.streamCtx = ctx
	n.streamCancel = cancel
}

// Retrieve the current stream context.
// This should be called with the stream lock held.
func (n *NetworkTransport) getStreamContext() context.Context {
	n.streamCtxLock.RLock()
	defer n.streamCtxLock.RUnlock()
	return n.streamCtx
}

// Handle inbound connections for all connection lifespan.
// When a new connection is established, the connector must pass the connection
// as well the connection context.
// The handler will exit when the context is cancelled or the connection is closed.
func (n *NetworkTransport) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	dec := codec.NewDecoder(r, &codec.MsgpackHandle{})
	enc := codec.NewEncoder(w, &codec.MsgpackHandle{})

	for {
		select {
		case <-ctx.Done():
			n.logger.Debug("stream is closed")
			return
		default:
		}

		if err := n.handleCommand(r, dec, enc); err != nil {
			if err != io.EOF {
				n.logger.Error("failed to decode incoming command. %v", err)
			}
			return
		}

		if err := w.Flush(); err != nil {
			n.logger.Error("failed to flush response. %v", err)
			return
		}
	}
}

// Handles an incoming command.
// It will decode and dispatch a single command.
func (n *NetworkTransport) handleCommand(r *bufio.Reader, dec *codec.Decoder, enc *codec.Encoder) error {
	rpcType, err := r.ReadByte()
	if err != nil {
		return err
	}

	respCh := make(chan RPCResponse, 1)
	rpc := RPC{
		RespChan: respCh,
	}

	switch rpcType {
	case rpcGMCast:
		var req GMCastRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}

		rpc.Command = &req
	case rpcCompute:
		var req ComputeRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
	case rpcGather:
		var req GatherRequest
		if err := dec.Decode(&req); err != nil {
			return err
		}
		rpc.Command = &req
	case rpcRecover:
		return errors.New("recover not ready yet")
	default:
		return fmt.Errorf("unknown rpc type %d", rpcType)
	}

	select {
	case n.consumeCh <- rpc:
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}

	select {
	case res := <-respCh:
		resErr := ""
		if res.Error != nil {
			resErr = res.Error.Error()
		}

		if err := enc.Encode(resErr); err != nil {
			return err
		}

		if err := enc.Encode(res.Response); err != nil {
			return err
		}
	case <-n.shutdownCh:
		return ErrTransportShutdown
	}
	return nil
}

func (n *NetworkTransport) genericRPC(id ServerID, target ServerAddress, rpcType uint8, req interface{}, res interface{}) error {
	conn, err := n.getConnFromAddressResolver(id, target)
	if err != nil {
		return err
	}

	if n.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(n.timeout))
	}

	if err = sendRPC(conn, rpcType, req); err != nil {
		return err
	}

	ok, err := decodeResponse(conn, res)
	if !ok {
		return err
	}

	n.returnConn(conn)
	return nil
}

// Retrieves a connection from the resolver if available, or defaults to a connection
// using the given target address.
func (n *NetworkTransport) getConnFromAddressResolver(id ServerID, target ServerAddress) (*netConn, error) {
	address := n.getAddressOrFallback(id, target)
	return n.getConn(address)
}

// Try to acquire a server address from the address resolver by the given id, if could not find use
// the target as a fallback value.
func (n *NetworkTransport) getAddressOrFallback(id ServerID, target ServerAddress) ServerAddress {
	if n.serverAddressResolver != nil {
		actual, err := n.serverAddressResolver.Resolve(id)
		if err == nil {
			return actual
		}
		n.logger.Warn("unable to get address for server, using fallback. id: %s, fallback %s. error: %v", id, target, err)
	}
	return target
}

// Get a new connection from the pool, if it not exists create a new one.
func (n *NetworkTransport) getConn(target ServerAddress) (*netConn, error) {
	if conn := n.getPooledConn(target); conn != nil {
		return conn, nil
	}

	conn, err := n.stream.Dial(target, n.timeout)
	if err != nil {
		return nil, err
	}

	netConn := &netConn{
		target: target,
		conn:   conn,
		r:      bufio.NewReader(conn),
		w:      bufio.NewWriter(conn),
	}

	// Setup encoder/decoders
	netConn.dec = codec.NewDecoder(netConn.r, &codec.MsgpackHandle{})
	netConn.enc = codec.NewEncoder(netConn.w, &codec.MsgpackHandle{})

	// Done
	return netConn, nil
}

// Returns back the connection to the pool, if the pool is exceeded the max size
// the connection will be released.
func (n *NetworkTransport) returnConn(conn *netConn) {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	key := conn.target
	availableConnections := n.connPool[key]

	if !n.IsShutdown() && len(availableConnections) < n.maxPool {
		n.connPool[key] = append(availableConnections, conn)
	} else {
		conn.Release()
	}
}

// Grab a connection to the given target from the connection pool.
func (n *NetworkTransport) getPooledConn(target ServerAddress) *netConn {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()

	availableConnections, ok := n.connPool[target]
	if !ok || len(availableConnections) == 0 {
		return nil
	}

	var conn *netConn
	size := len(availableConnections)
	conn, availableConnections[size-1] = availableConnections[size-1], nil
	n.connPool[target] = availableConnections[:size-1]
	return conn
}

// Verify if the transport is shutdown.
func (n *NetworkTransport) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// LocalAddress implements Transport interface.
func (n *NetworkTransport) LocalAddress() ServerAddress {
	return ServerAddress(n.stream.Addr().String())
}

// Implements the Transport interface.
func (n *NetworkTransport) GMCast(id ServerID, target ServerAddress, req *GMCastRequest, res *GMCastResponse) error {
	return n.genericRPC(id, target, rpcGMCast, req, res)
}

// Implements Transport interface
func (n *NetworkTransport) Compute(id ServerID, target ServerAddress, req *ComputeRequest, res *ComputeResponse) error {
	return n.genericRPC(id, target, rpcCompute, req, res)
}

// Implements Transport interface
func (n *NetworkTransport) Gather(id ServerID, target ServerAddress, req *GatherRequest, res *GatherResponse) error {
	return n.genericRPC(id, target, rpcGather, req, res)
}

// Shutdown the current transport.
func (n *NetworkTransport) Close() error {
	n.shutdownLock.Lock()
	defer n.shutdownLock.Unlock()

	if !n.shutdown {
		close(n.shutdownCh)
		n.stream.Close()
		n.shutdown = true
	}

	return nil
}

// Implements Transport consumer
func (n *NetworkTransport) Consumer() <-chan RPC {
	return n.consumeCh
}

// Encodes and send an RPC request.
func sendRPC(conn *netConn, rpcType uint8, req interface{}) error {
	// Write the request type
	if err := conn.w.WriteByte(rpcType); err != nil {
		conn.Release()
		return err
	}

	// Send the request
	if err := conn.enc.Encode(req); err != nil {
		conn.Release()
		return err
	}

	// Flush
	if err := conn.w.Flush(); err != nil {
		conn.Release()
		return err
	}
	return nil
}

// Decodes the responses sent on the connection
func decodeResponse(conn *netConn, resp interface{}) (bool, error) {
	// Decode the error if any
	var rpcError string
	if err := conn.dec.Decode(&rpcError); err != nil {
		conn.Release()
		return false, err
	}

	// Decode the response
	if err := conn.dec.Decode(resp); err != nil {
		conn.Release()
		return false, err
	}

	// Format an error if any
	if rpcError != "" {
		return true, fmt.Errorf(rpcError)
	}
	return true, nil
}
