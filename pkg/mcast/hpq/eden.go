package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

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

type EdenSharded struct {
	shard Shard
}

type WrappedMessageElement struct {
	Value types.Message
}

func NewEden(parent context.Context, deliver func(ElementNotification) bool) Eden {
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

func (e *EdenSharded) Enqueue(message types.Message) bool {
	return e.shard.Enqueue(wrap(message))
}

func (e *EdenSharded) Dequeue(message types.Message) interface{} {
	return unwrapOrNil(e.shard.Dequeue(wrap(message)))
}

func (e *EdenSharded) Apply(f func([]types.Message)) {
	e.shard.Apply(func(elements []ShardElement) {
		var unwrapped []types.Message
		for _, element := range elements {
			unwrapped = append(unwrapped, element.(WrappedMessageElement).Value)
		}

		f(unwrapped)
	})
}

func wrap(message types.Message) WrappedMessageElement {
	return WrappedMessageElement{Value: message}
}

func unwrapOrNil(element interface{}) interface{} {
	if element == nil {
		return nil
	}
	return element.(WrappedMessageElement).Value
}

func (w WrappedMessageElement) Id() interface{} {
	return w.Value.Identifier
}

func (w WrappedMessageElement) Less(item HeapItem) bool {
	other := item.(WrappedMessageElement)
	return w.Value.HasHigherPriority(other.Value)
}

func (w WrappedMessageElement) Diff(element QueueElement) bool {
	other := element.(WrappedMessageElement)
	return w.Value.Diff(other.Value)
}

func (w WrappedMessageElement) IsUpdatedVersion(element ShardElement) bool {
	other := element.(WrappedMessageElement)
	return w.Value.Updated(other.Value)
}

func (w WrappedMessageElement) IsAcceptable() bool {
	return w.Value.State != types.S3
}
