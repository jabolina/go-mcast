package protocol

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

// PreviousSet is used by the protocol for handling conflicts and
// ordering messages when needed.
type PreviousSet interface {
	// Append adds a message into the set.
	Append(message types.Message)

	// Clear the whole set of previous messages.
	Clear()

	// ExistsConflict will verify if for the given message exists
	// any message in the previous set that can conflict.
	ExistsConflict(types.Message) bool
}

type ConcurrentPreviousSet struct {
	// Mutex for operations synchronization.
	mutex *sync.Mutex

	// Conflict relationship is used to verify if exists
	// messages that do not commute.
	relationship types.ConflictRelationship

	// Set values.
	values map[types.UID]types.Message
}

// NewPreviousSet creates a new instance of the PreviousSet.
func NewPreviousSet(relationship types.ConflictRelationship) PreviousSet {
	return &ConcurrentPreviousSet{
		mutex:        &sync.Mutex{},
		relationship: relationship,
		values:       make(map[types.UID]types.Message),
	}
}

// Append implements the PreviousSet interface.
func (c *ConcurrentPreviousSet) Append(message types.Message) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.values[message.Identifier] = message
}

// Clear implements the PreviousSet interface.
func (c *ConcurrentPreviousSet) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for uid := range c.values {
		delete(c.values, uid)
	}

	c.values = make(map[types.UID]types.Message)
}

// ExistsConflict implements the PreviousSet interface.
func (c *ConcurrentPreviousSet) ExistsConflict(message types.Message) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, t := range c.values {
		if c.relationship.Conflict(message, t) {
			return true
		}
	}

	return false
}
