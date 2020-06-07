package mcast

// The protocol needs a global distributed clock to synchronize
// all groups when multiple groups are present on the protocol.
// This global clock implementation is delegated to the client, since
// we do not want to hold the final user to a implementation.
//
// For this clock is better to use a atomic broadcast implementation
// that works as an distributed atomic counter. Some implementations
// like etcd or the hashicorp raft implementation can be used here,
// but since there is configuration needed is better that the user
// using the protocol configures as wanted.
type LogicalGlobalClock interface {
	// The clock is increased.
	Tick()

	// The value present on the clock is retrieved.
	Tock() uint64

	// The value on the clock leaps to the given value.
	Leap(to uint64)
}
