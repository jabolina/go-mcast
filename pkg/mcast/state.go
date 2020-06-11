package mcast

import (
	"sync"
	"sync/atomic"
)

// A logical clock to provide the timestamp for the process group.
// Using atomic operations for thread safety amongst group members access.
type LogicalClock struct {
	// Logical operation index.
	index uint64
}

// Tick the clock, this will atomically add one to the index.
func (clk *LogicalClock) Tick() {
	atomic.AddUint64(&clk.index, 1)
}

// Atomically reads the timestamp.
func (clk *LogicalClock) Tock() uint64 {
	return atomic.LoadUint64(&clk.index)
}

// Atomically defines the value for the new one.
func (clk *LogicalClock) Leap(to uint64) {
	atomic.SwapUint64(&clk.index, to)
}

// Holds information about a single node. This node will
// be kept inside a group on nodes.
type NodeState struct {
	// Holds the information about the peer server.
	Server Server

	// This peer transport for communication
	Trans Transport
}

// A group provides a interface to work like a single unity but will
// actually be handling a group of replicated processes.
type GroupState struct {
	// Members of the local group.
	Nodes []NodeState

	// Clock for the group.
	Clk LogicalClock

	// Used to track spawned go routines.
	group *sync.WaitGroup
}

// Spawn a new goroutine and controls it with the wait group.
func (g *GroupState) emit(f func()) {
	g.group.Add(1)
	go func() {
		defer g.group.Done()
		f()
	}()
}
