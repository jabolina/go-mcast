package protocol

import (
	"sync/atomic"
)

// LogicalClock provide the timestamp for a single peer.
// Using atomic operations for thread safety across concurrent
// requests.
type LogicalClock interface {
	// Tick the clock is increased.
	Tick()

	// Tock the value present on the clock is retrieved.
	Tock() uint64

	// Leap the value on the clock leaps to the given value.
	Leap(to uint64)
}

// ProcessClock for a single process, implements the LogicalClock interface.
type ProcessClock struct {
	// Logical operation index.
	index uint64
}

// Tick Implements the LogicalClock interface.
func (p *ProcessClock) Tick() {
	atomic.AddUint64(&p.index, 1)
}

// Tock Implements the LogicalClock interface.
func (p *ProcessClock) Tock() uint64 {
	return atomic.LoadUint64(&p.index)
}

// Leap Implements the LogicalClock interface.
func (p *ProcessClock) Leap(to uint64) {
	atomic.StoreUint64(&p.index, to)
}

func NewClock() LogicalClock {
	return &ProcessClock{
		index: 0,
	}
}
