package internal

import (
	"errors"
	"fmt"
	"go-mcast/pkg/mcast"
	"sync"
)

var (
	// Err is returned when an RPC arrives in a version that the current
	// unity cannot handle.
	ErrUnsupportedProtocol = errors.New("protocol version not supported")
)

// Holds information for shutting down the whole group.
type poweroff struct {
	shutdown bool
	ch       chan struct{}
	mutex    sync.Mutex
}

// Unity is a group
type Unity struct {
	// Hold information for the group, the group acts like a unity.
	// The unity *must* have a majority of non-faulty members.
	state GroupState

	// Holds configuration about the unity. About the local group name,
	// logger utilities, protocol version, etc.
	configuration mcast.Config

	// The unity state machine to commit values.
	sm StateMachine

	// Store the local unity address.
	address mcast.ServerAddress

	// Transport layer for communication.
	trans mcast.Transport

	// Storage for storing information about the state machine.
	storage mcast.Storage

	// Shutdown channel to exit, protected to prevent concurrent exits.
	off poweroff
}

// Creates an RPC header to be sent across RPC requests.
func (u *Unity) getRPCHeader() mcast.RPCHeader {
	return mcast.RPCHeader{
		ProtocolVersion: u.configuration.Version,
	}
}

// Verify if the current version can handle the current RPC request.
func (u *Unity) checkRPCHeader(rpc mcast.RPC) error {
	h, ok := rpc.Command.(mcast.WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC doest not have a header")
	}

	header := h.GetRPCHeader()
	if header.ProtocolVersion > mcast.LatestProtocolVersion {
		return ErrUnsupportedProtocol
	}

	if header.ProtocolVersion != u.configuration.Version {
		return ErrUnsupportedProtocol
	}

	return nil
}


