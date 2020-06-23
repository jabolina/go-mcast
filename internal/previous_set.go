package internal

import "sync"

// Previous set used by the protocol for handling
// conflicts and ordering messages.
// This set *must* be thread safety.
type PreviousSet interface {
	// Add a message into the set.
	Append(message Message)

	// Clear the whole set.
	Clear()

	// Creates an snapshot of the messages present
	// on the previous set and returns as a slice.
	Snapshot() []Message
}

type ConcurrentPreviousSet struct {
	// Mutex for operations synchronization.
	mutex *sync.Mutex

	// Set values.
	values map[UID]Message
}

// Creates a new instance of the PreviousSet.
func NewPreviousSet() PreviousSet {
	return &ConcurrentPreviousSet{
		mutex:  &sync.Mutex{},
		values: make(map[UID]Message),
	}
}

// Implements the PreviousSet interface.
func (c *ConcurrentPreviousSet) Append(message Message) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.values[message.Identifier] = message
}

// Implements the PreviousSet interface.
func (c *ConcurrentPreviousSet) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for uid := range c.values {
		delete(c.values, uid)
	}

	c.values = make(map[UID]Message)
}

// Implements the PreviousSet interface.
func (c *ConcurrentPreviousSet) Snapshot() []Message {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	var messages []Message
	for _, message := range c.values {
		messages = append(messages, message)
	}
	return messages
}
