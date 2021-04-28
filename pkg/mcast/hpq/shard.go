package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
)

// A Shard interface.
type Shard interface {
	// Enqueue add a new item and returns true if a change
	// occurred and false otherwise.
	Enqueue(ShardElement) bool

	// Dequeue remove the given item from the queue.
	Dequeue(ShardElement) interface{}

	// Apply will read all the current available elements and
	// executing the given function.
	Apply(func([]ShardElement))
}

type ShardElement interface {
	QueueElement

	IsUpdatedVersion(ShardElement) bool

	IsAcceptable() bool
}

// PriorityQueueShard implements the Shard interface. This will be used by a single
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
// so the PriorityQueueShard responsibility will be to verify if the Value was
// no previously applied and send back through the deliver callback.
type PriorityQueueShard struct {
	// The parent context, this will be used to shutdown the
	// poll method and close the queue in a way not graceful.
	ctx context.Context

	// Channel that will receive information about the changes
	// in the head of the queue.
	headChange chan QueueElement

	// Actual Message values. The values will be kept
	// inside a sorted priorityQueue, so we can have the guarantee
	// of unique items while keeping the queue behaviour.
	priorityQueue IPriorityQueue

	// Deliver function to be executed when the head element changes.
	// We will be notified by the IPriorityQueue.
	deliver func(ElementNotification) bool
}

// NewShard create a new queue data structure.
func NewShard(ctx context.Context, deliver func(ElementNotification) bool, f func(ShardElement) bool) Shard {
	headChannel := make(chan QueueElement)
	r := &PriorityQueueShard{
		ctx:        ctx,
		headChange: headChannel,
		deliver:    deliver,
		priorityQueue: NewPriorityQueue(ctx, headChannel, func(element QueueElement) bool {
			return f(element.(ShardElement))
		}),
	}
	helper.InvokerInstance().Spawn(r.poll)
	return r
}

func (r *PriorityQueueShard) verifyAndDeliverHead(element ShardElement) {
	en := ElementNotification{
		Value:   element,
		OnApply: false,
	}

	if !r.deliver(en) {
		helper.InvokerInstance().Spawn(func() {
			r.Dequeue(element)
		})
	}
}

// This method will be polling while the application is
// alive. The element in the head of the queue will be
// verified every 5 milliseconds and if the element
// changed the delivery method will be called.
func (r *PriorityQueueShard) poll() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case m := <-r.headChange:
			r.verifyAndDeliverHead(m.(ShardElement))
		}
	}
}

// Will verify if the Message can be added into the priorityQueue.
// The cache will hold messages that already removed and
// cannot be inserted again.
func (r *PriorityQueueShard) verifyAndInsert(element ShardElement) bool {
	r.priorityQueue.Push(element)
	return true
}

// Enqueue Implements the Shard interface.
// This method will add the given element into the priorityQueue,
// if the Value already exists it will be updated and if
// there is need the values will be sorted again.
func (r *PriorityQueueShard) Enqueue(element ShardElement) bool {
	exists := r.priorityQueue.Get(element)
	if exists != nil {
		v := exists.(ShardElement)
		// I can only change the Message state if the new Message contains
		// a timestamp at lest equals to the already present on memory and
		// a state that is currently higher or equal than the previous one.
		// Thus ensuring that the Message do not "go back in time".
		if v.IsAcceptable() && v.IsUpdatedVersion(element) {
			return r.verifyAndInsert(element)
		}
		return false
	}
	return r.verifyAndInsert(element)
}

// Dequeue Implements the Shard interface.
func (r *PriorityQueueShard) Dequeue(element ShardElement) interface{} {
	return r.priorityQueue.Remove(element)
}

func (r *PriorityQueueShard) Apply(f func([]ShardElement)) {
	values := r.priorityQueue.Values()
	var res []ShardElement
	for _, value := range values {
		res = append(res, value.(ShardElement))
	}
	f(res)
}
