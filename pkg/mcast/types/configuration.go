package types

import (
	"context"
	"errors"
	"net"
	"time"
)

var (
	ErrInvalidName          = errors.New("invalid participant name")
	ErrInvalidPartition     = errors.New("invalid partition name")
	ErrInvalidAddress       = errors.New("invalid TCP address")
	ErrInvalidVersion       = errors.New("invalid protocol version")
	ErrConflictRelationship = errors.New("conflict relationship cannot be nil")
	ErrStorage              = errors.New("storage cannot be nil")
	ErrLogger               = errors.New("logger cannot be nil")
	ErrOracle               = errors.New("oracle cannot be nil")
)

// Holds the peer configuration.
type PeerConfiguration struct {
	// The peer name.
	Name PeerName

	// Which partition does this peer belongs to.
	Partition Partition

	// Address to bind the TCP socket.
	Address TCPAddress

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

	// Listener for commits.
	Commit chan<- Response

	// When executing an action we can have a timeout.
	ActionTimeout time.Duration
}

// The configuration for using the atomic multicast.
type Configuration struct {
	// The name for the current participant.
	// Used mostly for debugging purposes, this should be a unique
	// identifier of the protocol participant.
	Name PeerName

	// Identify the partition in which the current peer will be a
	// part of. The common way is to have multiple participants inside
	// a same partition.
	Partition Partition

	// Address to bind the TCP socket.
	Address TCPAddress

	// Which version of the protocol will be used.
	Version uint

	// Timeout to be applied on actions.
	DefaultTimeout time.Duration

	// The conflict relationship that will be used
	// to order the requests for delivery.
	Conflict ConflictRelationship

	// Stable storage to maintaining the state machine data.
	Storage Storage

	// Logger to be used by the protocol.
	Logger Logger

	// Oracle used to resolve participant addresses.
	Oracle Oracle
}

func (c Configuration) IsValid() error {
	if len(c.Name) == 0 {
		return ErrInvalidName
	}

	if len(c.Partition) == 0 {
		return ErrInvalidPartition
	}

	if len(c.Address) == 0 {
		return ErrInvalidAddress
	}

	host, _, err := net.SplitHostPort(string(c.Address))
	if err != nil {
		return err
	}
	if net.ParseIP(host) == nil {
		return ErrInvalidAddress
	}

	if c.Version < 0 || c.Version > LatestProtocolVersion {
		return ErrInvalidVersion
	}

	if c.Conflict == nil {
		return ErrConflictRelationship
	}

	if c.Storage == nil {
		return ErrStorage
	}

	if c.Logger == nil {
		return ErrLogger
	}

	if c.Oracle == nil {
		return ErrOracle
	}

	return nil
}
