package internal

import (
	"sync"
	"sync/atomic"
)

// Represents the status of the protocol node.
// The protocol itself does not require nodes states explicitly, but we will
// use to denote states as a helper.
type State uint8

const (
	// The node did not started yet.
	Off State = iota

	// The node is up and running
	On

	// The node was requested to shutdown and not done yet.
	Shutting

	// The node was requested to be turned off and is off.
	Shutdown
)

// A logical clock to provide the timestamp for the process group.
// Using atomic operations for thread safety amongst group members access.
type LogicalClock struct {
	// Logical operation index
	index uint64
}

// Tick the clock, this will atomically add one to the index
func (clk *LogicalClock) Tick() {
	atomic.AddUint64(&clk.index, 1)
}

// Atomically reads the timestamp
func (clk *LogicalClock) Tock() uint64 {
	return atomic.LoadUint64(&clk.index)
}

// Atomically defines the value for the new one
func (clk *LogicalClock) Defines(to uint64) {
	atomic.SwapUint64(&clk.index, to)
}

// Holds information about a single node. This node will
// be kept inside a group on nodes
type NodeState struct {
	// Defines which role the node should play
	Role State

	// Lock for operations for a single node
	mutex sync.Mutex
}

// A group provides a interface to work like a single unity but will
// actually be handling a group of replicated processes
type GroupState struct {
	// Members of the local group
	Nodes []NodeState

	// Clock for the group
	Clk LogicalClock
}
