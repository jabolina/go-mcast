package concurrent

import (
	"sync"
	"time"
)

// Detect routine starvation by validating how long
// a routine takes to complete or by verifying how
// long it takes between events.
type Detector struct {
	mutex   sync.Mutex
	timeout time.Duration
	mem     map[uint64]time.Time
}

func NewDetector(timeout time.Duration) *Detector {
	return &Detector{
		timeout: timeout,
		mem:     make(map[uint64]time.Time),
	}
}

// Reset the detector cleaning the old saved times.
func (d *Detector) Reset() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.mem = make(map[uint64]time.Time)
}

// For a given event verifies if it already happened before and if
// so verify if the timeouts already occurred. If the event never
// happened before it will be added into memory with the current
// time of verification.
// Returns a bool with true if the value still in a valid timeout
// duration, false if the value already exceeded the timeout configuration.
func (d *Detector) Happened(event uint64) (bool, time.Duration) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	ok := true
	now := time.Now()
	exceed := time.Duration(0)

	if old, happened := d.mem[event]; happened {
		exceed = now.Sub(old) - d.timeout
		if exceed > 0 {
			ok = false
		}
	}
	d.mem[event] = now
	return ok, exceed
}
