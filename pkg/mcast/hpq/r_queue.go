package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// IRQueue is the interface responsible for interacting with
// the received messages.
type IRQueue interface {
	// Acceptable verify if the given interface is eligible to be
	// added to the queue.
	Acceptable(types.Message) bool

	// Append enqueue a new item and returns true if a change
	// occurred and false otherwise.
	Append(types.Message) bool

	// Remove the given item from the queue.
	Remove(types.Message)

	// Apply will read all the current available elements and
	// executing the given function.
	Apply(func([]types.Message))
}

type ElementNotification struct {
	value interface{}
	onApply bool
}

type PeerQueueManager struct {

}

func NewReceivedQueue(ctx context.Context, notify chan<- ElementNotification) IRQueue {
	return nil
}

