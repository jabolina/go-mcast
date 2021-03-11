package types

// StateMachine is an interface that can be implemented
// to use the replicated value across replicas.
type StateMachine interface {
	// Commit the given entry into the state machine, turning it available for all clients.
	Commit(Message, bool) error

	// Return the history of commands for the state machine. See that the command history
	// will be returned at the time of request, so when processing the history new commands
	// may already exists.
	History() ([]Message, error)

	// Restores the state machine back to a given a state.
	Restore() error
}

// A in memory default value to be used.
type DefaultStateMachine struct {
	// Log structure where commands will be appended.
	log Log
}

// Commit the operation into the stable storage.
// Some operations will change values into the state machine
// while some other operations is just querying the state
// machine for values.
// All committed entries will be converted to a LogEntry and then
// appended to the Log structure, so the log will grow as time passes.
// After the entry is appended, the value will be passed to a stable storage
// and the listener will be notified about it.
//
// This method is a critical area, where *all* the steps must be executed as
// a transaction.
func (i *DefaultStateMachine) Commit(message Message, isGenericDeliver bool) error {
	return i.log.Append(message, isGenericDeliver)
}

func (i *DefaultStateMachine) History() ([]Message, error) {
	return i.log.Dump()
}

func (i *DefaultStateMachine) Restore() error {
	return nil
}

// Create the new state machine using the given storage
// for committing changes.
func NewStateMachine(log Log) *DefaultStateMachine {
	return &DefaultStateMachine{log: log}
}
