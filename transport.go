package go_mcast

import (
	"io"
	"net"
	"time"
)

// Captures a response/error from an RPC call.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// A context for an RPC call with a channel to obtain the response/error.
type RPC struct {
	Command  interface{}
	Reader   io.Reader
	RespChan chan<- RPCResponse
}

// Sends response back through channel.
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{
		Response: resp,
		Error:    err,
	}
}

type Transport interface {
	// Returns a channel that can be used to consume RPCResponses.
	Consumer() <-chan RPC

	// Local peer address.
	LocalAddr() ServerAddress

	// Serialize the peer address.
	EncodePeer(id ServerID, address ServerAddress) []byte

	// Decode the peer information and returns the address.
	DecodePeer([]byte) ServerAddress

	// Closes the transport.
	Close() error

	// Send the appropriate RPC request to the target peer.
	GMCast(id ServerID, target ServerAddress, req *GMCastRequest, res *GMCastResponse) error
}

// Interface that a Transport can provide which allows connections
// and disconnections.
type WithPeers interface {
	// Connect a peer
	Connect(peer ServerAddress)

	// Disconnect a peer
	Disconnect(peer ServerAddress)

	// Disconnect all peers
	DisconnectAll()
}


// Provides a low level stream abstraction for NetworkTransport
type StreamLayer interface {
	net.Listener
	Dial(address ServerAddress, timeout time.Duration) (net.Conn, error)
}
