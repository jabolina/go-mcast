package types

import "context"

// Holds the peer configuration.
type PeerConfiguration struct {
	// The peer name.
	Name string

	// Which partition does this peer belongs to.
	Partition Partition

	// Version at which the peer is working.
	Version uint

	// Conflict relationship, will be used to order the
	// delivery sequence.
	Conflict ConflictRelationship

	// Stable storage to commit the values of the state
	// machine.
	Storage Storage

	// Application context for handling goroutines.
	Ctx context.Context

	// Application function to cancel the context.
	Cancel context.CancelFunc
}

// The configuration for using the atomic multicast.
type Configuration struct {
	// The name for the current participant.
	// Used mostly for debugging purposes, this should be a unique
	// identifier of the protocol participant.
	Name Partition

	// Identify the partition in which the current peer will be a
	// part of. The common way is to have multiple participants inside
	// a same partition.
	Partition Partition

	// Which version of the protocol will be used.
	Version uint

	// The conflict relationship that will be used
	// to order the requests for delivery.
	Conflict ConflictRelationship

	// Stable storage to maintaining the state machine data.
	Storage Storage

	// Logger to be used by the protocol.
	Logger Logger
}
