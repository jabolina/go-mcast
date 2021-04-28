package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
)

type Eden interface {
	io.Closer

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
	delegate Queue
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewEden(parent context.Context, deliver func(ElementNotification) bool) Eden {
	ctx, cancel := context.WithCancel(parent)
	return &EdenSharded{
		delegate: NewQueue(ctx, deliver, func(m types.Message) bool {
			return m.State == types.S3
		}),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (e *EdenSharded) Close() error {
	e.cancel()
	return nil
}

func (e *EdenSharded) Enqueue(message types.Message) bool {
	return e.delegate.Enqueue(message)
}

func (e *EdenSharded) Dequeue(message types.Message) interface{} {
	return e.delegate.Dequeue(message)
}

func (e *EdenSharded) Apply(f func([]types.Message)) {
	e.delegate.Apply(f)
}
