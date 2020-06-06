package amcast

import (
	"go-mcast/internal/remote"
	"io"
)

// StateMachine is an interface that can be implemented
// to use the replicated value across replicas.
type StateMachine interface {
	// Commit the given entry into the state machine, turning it available for all clients.
	Commit(*remote.Entry) interface{}

	// Restores the state machine back to a given a state.
	Restore(closer io.ReadCloser) error
}
