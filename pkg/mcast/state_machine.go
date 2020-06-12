package mcast

import (
	"encoding/json"
	"errors"
	"io"
)

var (
	ErrCommandUnknown = errors.New("unknown command applied into state machine")
)

// StateMachine is an interface that can be implemented
// to use the replicated value across replicas.
type StateMachine interface {
	// Commit the given entry into the state machine, turning it available for all clients.
	Commit(*Entry) (interface{}, error)

	// Restores the state machine back to a given a state.
	Restore(closer io.ReadCloser) error
}

// A in memory default value to be used.
type InMemoryStateMachine struct {
	// State machine stable storage for committing
	store Storage
}

// Commit the entry into the stable storage.
func (i *InMemoryStateMachine) Commit(entry *Entry) (interface{}, error) {
	var holder DataHolder
	if err := json.Unmarshal(entry.Data, &holder); err != nil {
		return nil, err
	}

	switch holder.Operation {
	// Some entry will be changed.
	case Command:
		message := Message{
			MessageState: S3,
			Timestamp:    entry.FinalTimestamp,
			Data:         holder.Content,
			Extensions:   entry.Extensions,
		}
		data, err := json.Marshal(message)
		if err != nil {
			return nil, err
		}
		if err := i.store.Set([]byte(holder.Key), data); err != nil {
			return nil, err
		}
		return message, nil
	// Some entry is read.
	case Query:
		data, err := i.store.Get([]byte(holder.Key))
		if err != nil {
			return nil, err
		}
		var message Message
		if err := json.Unmarshal(data, &message); err != nil {
			return nil, err
		}
		return message, nil
	default:
		return nil, ErrCommandUnknown
	}
}

func (i *InMemoryStateMachine) Restore(closer io.ReadCloser) error {
	panic("implement me")
}

// Create the new state machine using the given storage
// for committing changes.
func NewStateMachine(storage Storage) *InMemoryStateMachine {
	return &InMemoryStateMachine{store: storage}
}
