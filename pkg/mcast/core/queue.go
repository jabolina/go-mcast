package core

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
	"time"
)

// A Queue interface.
type Queue interface {
	// Add a new item and returns true if a change
	// occurred and false otherwise.
	Enqueue(interface{}) bool

	// Remove the given item from the queue.
	Dequeue(interface{}) interface{}

	// Subscribe a function to be executed when the
	// head element changes.
	Subscribe(func(interface{}))

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
// will be used a sorted set to retain the messages, using this
// approach when can have as faster delivery process, since we
// need only to verify the message on the head of the queue,
// and since the data structure is a set, there is the guarantee
// that only a single element will exists.
//
// The set will be sorted following the rules applied to deliver
// the messages, which means:
//
// 1 - First sort messages by the given Timestamp;
// 2 - If two messages have the same Timestamp, sort by the UID.
//
// Following this approach, in the head of the set will always be
// the probably next message to be delivered, since it will contain
// the lowest timestamp, when the head arrives on State S3 it will
// be delivered exactly once.
type RQueue struct {
	// The parent context, this will be used to shutdown the
	// poll method and close the queue in a way not graceful.
	ctx context.Context

	// Synchronization for operations applied on the set.
	mutex *sync.Mutex

	// Channel that will receive information about the changes
	// in the head of the queue.
	headChange chan types.Message

	// Keep track of messages that where already applied
	// onto the state machine.
	applied Cache

	// Actual message values. The values will be kept
	// inside a sorted set, so we can have the guarantee
	// of unique items while keeping the queue behaviour.
	set RecvQueue

	// Hold the conflict relationship to be used
	// when delivering messages.
	conflict types.ConflictRelationship

	// Deliver function to be executed when the head element
	// changes.
	deliver func(interface{})

	// Last known item on the head, used to ensure an exactly
	// once deliver of the messages.
	// The deliver will be called only when a change occurs in
	// the head element.
	lastHead interface{}
}

// Create a new queue data structure.
func NewQueue(ctx context.Context, conflict types.ConflictRelationship, f func(interface{})) Queue {
	headChannel := make(chan types.Message)
	r := &RQueue{
		ctx:        ctx,
		mutex:      &sync.Mutex{},
		conflict:   conflict,
		applied:    NewTtlCache(ctx),
		headChange: headChannel,
		deliver:    f,
		set:        NewPriorityQueue(headChannel, func(m types.Message) bool {
			return m.State == types.S3
		}),
	}
	InvokerInstance().Spawn(r.poll)
	return r
}

// Verify if the given two messages are different.
// This has a step further to help in the delivery process,
// where is also verified if the current message in the
// queue head is on State S3, if so, the message is ready
// to be delivered.
func (r RQueue) validateMessageChange(after, before types.Message) bool {
	if after.Identifier != before.Identifier || after.State != before.State || after.Timestamp < before.Timestamp {
		return before.State == types.S3
	}
	return false
}

// This method will verify if the given message was
// previously applied to the state machine.
// The values are held by a cache where each key can
// live up to 10 minutes.
func (r *RQueue) IsEligible(i interface{}) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	m := i.(types.Message)
	return !r.applied.Contains(string(m.Identifier))
}

func (r RQueue) verifyAndDeliverHead(message types.Message) {
	if r.IsEligible(message) {
		r.mutex.Lock()
		defer r.mutex.Unlock()
		r.applied.Set(string(message.Identifier))
		r.deliver(message)
	}
	r.set.Pop()
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
		case m :=<- r.headChange:
			InvokerInstance().Spawn(func() {
				r.verifyAndDeliverHead(m)
			})
		case <-time.After(10 * time.Second):
			r.set.Values()
		}
	}
}

// Will verify if the message can be added into the set.
// The cache will hold messages that already removed and
// cannot be inserted again.
func (r *RQueue) verifyAndInsert(message types.Message) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.set.Push(message)
	return true
}

// Implements the Queue interface.
// This method will add the given element into the set,
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
		if v.Timestamp <= m.Timestamp && v.State <= m.State {
			return r.verifyAndInsert(m)
		}
		return false
	}
	return r.verifyAndInsert(m)
}

// Implements the Queue interface.
// Insert a function that will be called when the
// element at the head of the queue changes.
func (r *RQueue) Subscribe(f func(interface{})) {
	r.deliver = f
}

// Implements the Queue interface.
func (r *RQueue) Dequeue(i interface{}) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	m := i.(types.Message)
	value := r.set.GetByKey(m.Identifier)
	if value != nil {
		r.applied.Set(string(m.Identifier))
		r.set.Remove(m.Identifier)
		return value
	}
	return nil
}

// Implements the Queue interface.
func (r *RQueue) GetIfExists(id string) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	v := r.set.GetByKey(types.UID(id))
	if v != nil {
		return *v
	}
	return nil
}

// Implements the Queue interface.
func (r *RQueue) GenericDeliver(i interface{}) {
	if !r.IsEligible(i) {
		return
	}

	message := i.(types.Message)
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var messages []types.Message
	for _, value := range r.set.Values() {
		if value.Identifier != message.Identifier {
			messages = append(messages, value)
		}
	}

	// If the message do not conflict with any other message
	// then it can be delivered directly.
	if !r.conflict.Conflict(message, messages) {
		r.Dequeue(message)
		r.deliver(message)
	}
}
