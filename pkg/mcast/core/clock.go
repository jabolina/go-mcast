package core

import (
	"sync/atomic"
)

// A logical clock to provide the timestamp for a single peer.
// Using atomic operations for thread safety across concurrent
// requests.
type LogicalClock interface {
	// The clock is increased.
	Tick()

	// The value present on the clock is retrieved.
	Tock() uint64

	// The value on the clock leaps to the given value.
	Leap(to uint64)
}

// Logical clock for a single process, implements the
// LogicalClock interface.
type ProcessClock struct {
	// Logical operation index.
	index uint64
}

// Implements the LogicalClock interface.
func (p *ProcessClock) Tick() {
	atomic.AddUint64(&p.index, 1)
}

// Implements the LogicalClock interface.
func (p *ProcessClock) Tock() uint64 {
	return atomic.LoadUint64(&p.index)
}

// Implements the LogicalClock interface.
func (p *ProcessClock) Leap(to uint64) {
	atomic.StoreUint64(&p.index, to)
}

func NewClock() LogicalClock {
	return &ProcessClock{
		index: 0,
	}
}
