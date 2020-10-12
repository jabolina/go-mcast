package core

import (
	"sync"
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
	// Sync access to the clock value.
	mutex *sync.Mutex

	// Logical operation index.
	index uint64
}

// Implements the LogicalClock interface.
func (p *ProcessClock) Tick() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.index += 1
}

// Implements the LogicalClock interface.
func (p *ProcessClock) Tock() uint64 {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.index
}

// Implements the LogicalClock interface.
func (p *ProcessClock) Leap(to uint64) {
	p.mutex.Lock()
	p.mutex.Unlock()
	p.index = to
}

func NewClock() LogicalClock {
	return &ProcessClock{
		mutex: &sync.Mutex{},
		index: 0,
	}
}
