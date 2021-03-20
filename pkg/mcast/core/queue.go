package core

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

// A Queue interface.
type Queue interface {
	// Add a new item and returns true if a change
	// occurred and false otherwise.
	Enqueue(interface{}) bool

	// Remove the given item from the queue.
	Dequeue(interface{}) interface{}

	// Remove the head of the queue.
	Pop() interface{}

	// Get the element if it exists on the memory.
	GetIfExists(id string) interface{}

	// This method is what turns the protocol into its generic
	// form, where not all messages are sorted.
	// This will verify if the given message conflict with other
	// messages and will delivery if possible.
	//
	// A message will only be able to be delivered if is on state
	// S3 and do not conflict with any other messages.
	GenericDeliver(interface{})

	// Verify if the given interface is eligible to be added
	// to the queue.
	IsEligible(interface{}) bool
}

// Implements the queue interface. This will be used by a single
// peer to hold information about processing messages. Internally
// will be used a priority queue to retain the messages, using this
// approach when can have as faster delivery process, since we
// need only to verify the message on the head of the queue,
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
// the probably next message to be delivered, since it will contain
// the lowest timestamp, when the head arrives on State S3 it will
// be delivered exactly once. Using the implemented Q,
// we will receive updates about changes on the head of the queue,
// so the RQueue responsibility will be to verify if the value was
// no previously applied and send back through the deliver callback.
type RQueue struct {
	// The parent context, this will be used to shutdown the
	// poll method and close the queue in a way not graceful.
	ctx context.Context

	// Synchronization for operations applied on the priorityQueue.
	mutex *sync.Mutex

	// Channel that will receive information about the changes
	// in the head of the queue.
	headChange chan types.Message

	// Keep track of messages that where already applied
	// onto the state machine.
	applied Cache

	// Actual message values. The values will be kept
	// inside a sorted priorityQueue, so we can have the guarantee
	// of unique items while keeping the queue behaviour.
	priorityQueue types.ReceivedQueue

	// Hold the conflict relationship to be used
	// when delivering messages.
	conflict types.ConflictRelationship

	// Deliver function to be executed when the head element changes.
	// We will be notified by the Q.
	deliver chan<- deliverRequest
}

// Create a new queue data structure.
func NewQueue(ctx context.Context, conflict types.ConflictRelationship, deliver chan<- deliverRequest) Queue {
	headChannel := make(chan types.Message)
	r := &RQueue{
		ctx:        ctx,
		mutex:      &sync.Mutex{},
		conflict:   conflict,
		applied:    NewTtlCache(ctx),
		headChange: headChannel,
		deliver:    deliver,
		priorityQueue: NewPriorityQueue(headChannel, func(m types.Message) bool {
			return m.State == types.S3
		}),
	}
	InvokerInstance().Spawn(r.poll)
	return r
}

// This method will verify if the given message was
// previously applied to the state machine.
// The values are held by a cache where each key can
// live up to 10 minutes.
func (r *RQueue) IsEligible(i interface{}) bool {
	m := i.(types.Message)
	return !r.applied.Contains(string(m.Identifier))
}

func (r *RQueue) verifyAndDeliverHead(message types.Message) {
	if r.applied.Set(string(message.Identifier)) {
		r.deliver <- deliverRequest{
			message: message,
			generic: false,
		}
	} else {
		InvokerInstance().Spawn(func() {
			r.Dequeue(message)
		})
	}
}

// This method will be polling while the application is
// alive. The element in the head of the queue will be
// verified every 5 milliseconds and if the element
// changed the delivery method will be called.
func (r *RQueue) poll() {
	defer close(r.deliver)
	for {
		select {
		case <-r.ctx.Done():
			return
		case m := <-r.headChange:
			r.verifyAndDeliverHead(m)
		}
	}
}

// Will verify if the message can be added into the priorityQueue.
// The cache will hold messages that already removed and
// cannot be inserted again.
func (r *RQueue) verifyAndInsert(message types.Message) bool {
	r.priorityQueue.Push(message)
	return true
}

// Implements the Queue interface.
// This method will add the given element into the priorityQueue,
// if the value already exists it will be updated and if
// there is need the values will be sorted again.
func (r *RQueue) Enqueue(i interface{}) bool {
	m := i.(types.Message)
	if !r.IsEligible(m) {
		return false
	}
	exists := r.GetIfExists(string(m.Identifier))
	if exists != nil {
		v := exists.(types.Message)
		// I can only change the message state if the new message contains
		// a timestamp at lest equals to the already present on memory and
		// a state that is currently higher or equal than the previous one.
		// Thus ensuring that the message do not "go back in time".
		if v.State != types.S3 && v.Updated(m) {
			return r.verifyAndInsert(m)
		}
		return false
	}
	return r.verifyAndInsert(m)
}

// Implements the Queue interface.
func (r *RQueue) Dequeue(i interface{}) interface{} {
	m := i.(types.Message)
	return r.priorityQueue.Remove(m.Identifier)
}

// Implements the Queue interface.
func (r *RQueue) Pop() interface{} {
	return r.priorityQueue.Pop()
}

// Implements the Queue interface.
func (r *RQueue) GetIfExists(id string) interface{} {
	v, ok := r.priorityQueue.GetByKey(types.UID(id))
	if ok {
		return v
	}
	return nil
}

// Implements the Queue interface.
func (r *RQueue) GenericDeliver(i interface{}) {
	if !r.IsEligible(i) {
		return
	}

	message := i.(types.Message)
	var messages []types.Message
	// This will copy the slice at the time of read.
	// This method does not guarantee that we have the
	// latest object version, the elements can change after
	// we read the slice.
	// Since the elements that will be delivered here could
	// be delivered at any order, this should not be a problem.
	for _, value := range r.priorityQueue.Values() {
		if value.Identifier != message.Identifier {
			messages = append(messages, value)
		}
	}

	// If the message do not conflict with any other message
	// then it can be delivered directly.
	if !r.conflict.Conflict(message, messages) && r.applied.Set(string(message.Identifier)) {
		r.deliver <- deliverRequest{
			message: message,
			generic: true,
		}
	}
}
