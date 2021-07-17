package network

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
)

// The reliableTransport interface providing the communication
// primitives by the protocol.
type Transport interface {
	io.Closer

	// Reliably deliver the message to all correct processes
	// in the same order.
	Broadcast(message types.Message) error

	// Unicast the message to a single partition.
	// This do not need to be a reliable reliableTransport, since
	// a partition contains a majority of correct processes
	// at least 1 process will receive the message.
	Unicast(message types.Message, partition types.Partition) error

	// Listen for messages that arrives on the reliableTransport.
	Listen() <-chan types.Message
}
