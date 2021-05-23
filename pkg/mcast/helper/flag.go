package helper

import "sync/atomic"

const (
	active   = 0x0
	inactive = 0x1
)

// The Flag structure.
// Only some transitions are available when using this structure,
// so this will not act as an atomic boolean. The accepted transitions are:
//
// IsActive if flag is 0x0, returns `true`;
// IsInactive if flag is 0x1, returns `true`;
// Inactivate iff IsActive change value to IsInactive.
//
// The start value will be `active`.
type Flag struct {
	// Holds the current state of the flag.
	flag int32
}

// IsActive returns `true` if the flag still active.
func (f *Flag) IsActive() bool {
	return atomic.LoadInt32(&f.flag) == active
}

// IsInactive returns `true` if the flag is inactive.
func (f *Flag) IsInactive() bool {
	return atomic.LoadInt32(&f.flag) == inactive
}

// Inactivate will inactivate the flag.
// Returns `true` if was active and now is inactive and returns `false`
// if it was already inactivated.
func (f *Flag) Inactivate() bool {
	return atomic.CompareAndSwapInt32(&f.flag, active, inactive)
}
