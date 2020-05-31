package mcast

import (
	"errors"
	"github.com/hashicorp/go-hclog"
	"io"
	"net"
	"time"
)

var (
	ErrorNotAdvertiseAddress = errors.New("local bind address not advertised")
	ErrorNotTCP              = errors.New("local address is not TCP")
)

type Creator func(stream StreamLayer) *NetworkTransport

// Implements StreamLayer for plain TCP
type TCPStreamLayer struct {
	advertise net.Addr
	listener  *net.TCPListener
}

// NewTCPTransport returns a NetworkTransport that is built on top of
// a TCP streaming transport layer.
func NewTCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

// NewTCPTransportWithLogger returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, with log output going to the supplied Logger
func NewTCPTransportWithLogger(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logger hclog.Logger,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		return NewNetworkTransportWithLogger(stream, maxPool, timeout, logger)
	})
}

// NewTCPTransportWithConfig returns a NetworkTransport that is built on top of
// a TCP streaming transport layer, using the given config struct.
func NewTCPTransportWithConfig(
	bindAddr string,
	advertise net.Addr,
	config *NetworkTransportConfig,
) (*NetworkTransport, error) {
	return newTCPTransport(bindAddr, advertise, func(stream StreamLayer) *NetworkTransport {
		config.Stream = stream
		return NewNetworkTransportWithConfig(config)
	})
}

func newTCPTransport(addr string, advertise net.Addr, factory Creator) (*NetworkTransport, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	stream := &TCPStreamLayer{
		advertise: advertise,
		listener:  lis.(*net.TCPListener),
	}

	available, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		lis.Close()
		return nil, ErrorNotTCP
	}

	if available.IP.IsUnspecified() {
		lis.Close()
		return nil, ErrorNotAdvertiseAddress
	}

	trans := factory(stream)
	return trans, nil
}

// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.listener.Accept()
}

// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.listener.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	// Use an advertise addr if provided
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}
