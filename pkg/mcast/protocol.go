package mcast

import (
	"fmt"
	"sync"
	"time"
)

// Holds information for shutting down the whole group.
type poweroff struct {
	shutdown bool
	ch       chan bool
	mutex    *sync.Mutex
}

// Unity is a group
type Unity struct {
	// Hold information for the group, the group acts like a unity.
	// The unity *must* have a majority of non-faulty members.
	State *GroupState

	// PreviousSet is the protocol needed for conflict evaluation.
	previousSet *PreviousSet

	// Holds configuration about the unity. About the local group name,
	// logger utilities, protocol version, etc.
	configuration *BaseConfiguration

	// The global clock that can be used to synchronize groups.
	clock LogicalGlobalClock

	// Timeout for requests computation.
	timeout time.Duration

	// Handle for committing response into the unity state machine.
	deliver *Deliver

	// Shutdown channel to exit, protected to prevent concurrent exits.
	off poweroff
}

func NewUnity(base *BaseConfiguration, cluster *ClusterConfiguration, storage Storage, clock LogicalGlobalClock) (*Unity, error) {
	off := poweroff{
		shutdown: false,
		ch:       make(chan bool),
		mutex:    &sync.Mutex{},
	}
	set := NewPreviousSet()
	unity := &Unity{
		previousSet:   set,
		configuration: base,
		timeout:       cluster.TransportConfiguration.Timeout,
		deliver: 	   NewDeliver(storage, set, base.Logger),
		clock:         clock,
		off:           off,
	}
	state, err := BootstrapGroup(base, cluster, unity)
	if err != nil {
		return nil, err
	}
	unity.State = state
	unity.State.emit(unity.run)

	return unity, nil
}

// Creates an RPC header to be sent across RPC requests.
func (u *Unity) GetRPCHeader() RPCHeader {
	return RPCHeader{
		ProtocolVersion: u.configuration.Version,
	}
}

// Verify if the current version can handle the current RPC request.
func (u *Unity) checkRPCHeader(rpc RPC) error {
	h, ok := rpc.Command.(WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC does not have a header")
	}

	header := h.GetRPCHeader()
	if header.ProtocolVersion > LatestProtocolVersion {
		return ErrUnsupportedProtocol
	}

	if header.ProtocolVersion != u.configuration.Version {
		return ErrUnsupportedProtocol
	}

	return nil
}

func (u *Unity) run() {
	for _, peer := range u.State.Nodes {
		u.State.emit(peer.Poll)
	}

	for {
		select {
		case <-u.off.ch:
			// handle poweroff
			return
		default:
		}
	}
}

// When an information for the unity is needed,
// use information directly from one of the peers.
func (u *Unity) ResolvePeer() *Peer {
	return u.State.Next()
}

func (u *Unity) GlobalClock() LogicalGlobalClock {
	return u.clock
}

// Shutdown all current spawned goroutines and returns
// a blocking future to wait for the complete shutdown.
func (u *Unity) Shutdown() Future {
	u.off.mutex.Lock()
	defer func() {
		close(u.off.ch)
		u.off.mutex.Unlock()
	}()

	if !u.off.shutdown {
		u.off.ch <- true
		u.off.shutdown = true
		return &ShutdownFuture{unity: u}
	}
	return &ShutdownFuture{unity: nil}
}
