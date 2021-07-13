package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// The Eden interface is the place where newborn and messages that are
// currently processed by the protocol will reside. This interface has
// fewer methods in comparison with the Memory, since the current Eden
// wil handle only a subpart of the messages.
//
// In order to interact with the queue this interface **must** be used.
type Eden interface {
	// Enqueue add a new item and returns true if a change
	// occurred and false otherwise.
	Enqueue(message types.Message) bool

	// Dequeue remove the given item from the queue.
	Dequeue(message types.Message) interface{}

	// Apply will read all the current available elements and
	// executing the given function.
	Apply(func([]types.Message))
}

// The EdenSharded is the concrete implementation of the Eden interface.
// It is called sharded because the structure could be broken into many
// disjoint shards, and each shard holds a priority queue.
//
// At the moment exists only a single shard, so there is only a single
// priority queue inside the Eden. To any element beyond this point will
// be declared as interface and is casted to a concrete element after the
// action is applied.
type EdenSharded struct {
	// Each shard can contain a single queue and is invoking the correct
	// shard that an action can be executed.
	shard Shard
}

// The WrappedMessageElement wraps the real message element.
// Inside the Shard, the elements must implement an interface. The WrappedMessageElement
// wraps the real element that is interest for the Memory client, and implements
// the required methods to use the Shard queue.
type WrappedMessageElement struct {
	// Value is the concrete element of interest.
	Value types.Message
}

func NewEden(parent context.Context, deliver func(ElementNotification) bool) Eden {
	// The filter method is used by the queue to verify if the element should
	// notify or not. We want only notification about elements on state `types.S3`,
	// these elements are ready to be delivered.
	filter := func(element ShardElement) bool {
		return element.(WrappedMessageElement).Value.State == types.S3
	}
	unwrappedDeliver := func(notification ElementNotification) bool {
		content := notification.Value.(WrappedMessageElement).Value
		en := ElementNotification{
			Value:   content,
			OnApply: notification.OnApply,
		}
		return deliver(en)
	}
	return &EdenSharded{
		shard: NewShard(parent, unwrappedDeliver, filter),
	}
}

// Enqueue implements the Eden interface.
// This will wrap the given message into the WrappedMessageElement and send
// to the queue using the shard.
func (e *EdenSharded) Enqueue(message types.Message) bool {
	return e.shard.Enqueue(wrap(message))
}

// Dequeue implements the Eden interface.
// Dequeue will receive a value from the shard and will unwrap the value
// from the WrappedMessageElement.
func (e *EdenSharded) Dequeue(message types.Message) interface{} {
	return unwrapOrNil(e.shard.Dequeue(wrap(message)))
}

// Apply implements the Eden interface.
// Through this method is possible to interact with the values inside the
// shard queue.
func (e *EdenSharded) Apply(f func([]types.Message)) {
	e.shard.Apply(func(elements []ShardElement) {
		var unwrapped []types.Message
		for _, element := range elements {
			unwrapped = append(unwrapped, element.(WrappedMessageElement).Value)
		}

		f(unwrapped)
	})
}

// Given the message value, this method will wrap the element inside
// the WrappedMessageElement.
func wrap(message types.Message) WrappedMessageElement {
	return WrappedMessageElement{Value: message}
}

// This method will receive an interface and will try to unwrap the
// value. If the interface is nil then returns nil, otherwise cast
// to WrappedMessageElement and return the message element.
func unwrapOrNil(element interface{}) interface{} {
	if element == nil {
		return nil
	}
	return element.(WrappedMessageElement).Value
}

// Id implements the ShardElement interface.
func (w WrappedMessageElement) Id() interface{} {
	return w.Value.Identifier
}

// Less implements the ShardElement interface.
func (w WrappedMessageElement) Less(item HeapItem) bool {
	other := item.(WrappedMessageElement)
	return w.Value.HasHigherPriority(other.Value)
}

// Diff implements the ShardElement interface.
func (w WrappedMessageElement) Diff(element QueueElement) bool {
	other := element.(WrappedMessageElement)
	return w.Value.Diff(other.Value)
}

// IsUpdatedVersion implements the ShardElement interface.
func (w WrappedMessageElement) IsUpdatedVersion(element ShardElement) bool {
	other := element.(WrappedMessageElement)
	return w.Value.Updated(other.Value)
}

// IsAcceptable implements the ShardElement interface.
func (w WrappedMessageElement) IsAcceptable() bool {
	return w.Value.State != types.S3
}
