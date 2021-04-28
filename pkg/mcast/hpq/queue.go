package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// A Queue interface.
type Queue interface {
	// Enqueue add a new item and returns true if a change
	// occurred and false otherwise.
	Enqueue(message types.Message) bool

	// Dequeue remove the given item from the queue.
	Dequeue(message types.Message) interface{}

	// Apply will read all the current available elements and
	// executing the given function.
	Apply(func([]types.Message))
}

// RQueue implements the queue interface. This will be used by a single
// peer to hold information about processing messages. Internally
// will be used a priority queue to retain the messages, using this
// approach when can have as faster delivery process, since we
// need only to verify the Message on the head of the queue,
// and since the data structure is a priority queue, we will have a
// sorted collection, with the element at the head the probably next
// element to be delivered.
//
// The priorityQueue will be sorted following the rules applied to deliver
// the messages, which means:
//
// 1 - First sort messages by the given Timestamp;
// 2 - If two messages have the same Timestamp, sort by the UID.
//
// Following this approach, in the head of the priorityQueue will always be
// the probably next Message to be delivered, since it will contain
// the lowest timestamp, when the head arrives on State S3 it will
// be delivered exactly once. Using the implemented Q,
// we will receive updates about changes on the head of the queue,
// so the RQueue responsibility will be to verify if the Value was
// no previously applied and send back through the deliver callback.
type RQueue struct {
	// The parent context, this will be used to shutdown the
	// poll method and close the queue in a way not graceful.
	ctx context.Context

	// Channel that will receive information about the changes
	// in the head of the queue.
	headChange chan types.Message

	// Actual Message values. The values will be kept
	// inside a sorted priorityQueue, so we can have the guarantee
	// of unique items while keeping the queue behaviour.
	priorityQueue IPriorityQueue

	// Deliver function to be executed when the head element changes.
	// We will be notified by the IPriorityQueue.
	deliver func(ElementNotification) bool
}

// NewQueue create a new queue data structure.
func NewQueue(ctx context.Context, deliver func(ElementNotification) bool, f func(types.Message) bool) Queue {
	headChannel := make(chan types.Message)
	r := &RQueue{
		ctx:           ctx,
		headChange:    headChannel,
		deliver:       deliver,
		priorityQueue: NewPriorityQueue(ctx, headChannel, f),
	}
	helper.InvokerInstance().Spawn(r.poll)
	return r
}

func (r *RQueue) verifyAndDeliverHead(message types.Message) {
	en := ElementNotification{
		Value:   message,
		OnApply: false,
	}

	if !r.deliver(en) {
		helper.InvokerInstance().Spawn(func() {
			r.Dequeue(message)
		})
	}
}

// This method will be polling while the application is
// alive. The element in the head of the queue will be
// verified every 5 milliseconds and if the element
// changed the delivery method will be called.
func (r *RQueue) poll() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case m := <-r.headChange:
			r.verifyAndDeliverHead(m)
		}
	}
}

// Will verify if the Message can be added into the priorityQueue.
// The cache will hold messages that already removed and
// cannot be inserted again.
func (r *RQueue) verifyAndInsert(message types.Message) bool {
	r.priorityQueue.Push(message)
	return true
}

// Enqueue Implements the Queue interface.
// This method will add the given element into the priorityQueue,
// if the Value already exists it will be updated and if
// there is need the values will be sorted again.
func (r *RQueue) Enqueue(m types.Message) bool {
	exists := r.GetIfExists(string(m.Identifier))
	if exists != nil {
		v := exists.(types.Message)
		// I can only change the Message state if the new Message contains
		// a timestamp at lest equals to the already present on memory and
		// a state that is currently higher or equal than the previous one.
		// Thus ensuring that the Message do not "go back in time".
		if v.State != types.S3 && v.Updated(m) {
			return r.verifyAndInsert(m)
		}
		return false
	}
	return r.verifyAndInsert(m)
}

// Dequeue Implements the Queue interface.
func (r *RQueue) Dequeue(m types.Message) interface{} {
	return r.priorityQueue.Remove(m.Identifier)
}

// GetIfExists Implements the Queue interface.
func (r *RQueue) GetIfExists(id string) interface{} {
	return r.priorityQueue.Get(types.UID(id))
}

func (r *RQueue) Apply(f func([]types.Message)) {
	values := r.priorityQueue.Values()
	f(values)
}
