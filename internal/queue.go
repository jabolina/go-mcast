package internal

import (
	"context"
	"github.com/ReneKroon/ttlcache"
	"github.com/wangjia184/sortedset"
	"log"
	"sync"
	"time"
)

// A Queue interface.
type Queue interface {
	// Add a new item.
	Enqueue(interface{})

	// Pick the first item from the queue.
	Pick() interface{}

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

	// Keep track of messages that where already applied
	// onto the state machine.
	applied *ttlcache.Cache

	// Actual message values. The values will be kept
	// inside a sorted set, so we can have the guarantee
	// of unique items while keeping the queue behaviour.
	set *sortedset.SortedSet

	// Hold the conflict relationship to be used
	// when delivering messages.
	conflict ConflictRelationship

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
func NewQueue(ctx context.Context, invoker *Invoker, conflict ConflictRelationship, f func(interface{})) Queue {
	c := ttlcache.NewCache()
	c.SetTTL(10 * time.Minute)
	r := &RQueue{
		ctx:      ctx,
		mutex:    &sync.Mutex{},
		conflict: conflict,
		applied:  c,
		set:      sortedset.New(),
		deliver:  f,
	}
	invoker.invoke(r.poll)
	return r
}

// Verify if the given two messages are different.
// This has a step further to help in the delivery process,
// where is also verified if the current message in the
// queue head is on State S3, if so, the message is ready
// to be delivered.
func (r RQueue) validateMessageChange(after, before Message) bool {
	if after.Identifier != before.Identifier || after.State != before.State || after.Timestamp < before.Timestamp {
		return before.State == S3
	}
	return false
}

// This method will verify if the element at the head
// of the set is ready to be delivered. Since the set
// already sorts the messages at the right order, we
// only need that the element at the head of the set
// be on State S3.
// If the message is ready, the subscribed deliver method
// will be called with the element, once the message is
// delivered it will also be removed from the set.
func (r *RQueue) verifyAndDeliver() {
	curr := r.Pick()

	// If the head still empty, no work to do.
	if curr == nil {
		return
	}

	// If the head is not empty, but the last element
	// I know is empty, so the head changed. If the new
	// element is on State S3 it is ready to be delivered.
	if r.lastHead == nil {
		r.lastHead = curr
		v := curr.(Message)
		if v.State == S3 {
			r.Dequeue(curr)
			r.deliver(curr)
		}
		return
	}

	// I already knew an element and the head looks like to have an
	// element, must verify if they are different.
	if r.validateMessageChange(r.lastHead.(Message), curr.(Message)) {
		r.lastHead = curr
		r.Dequeue(curr)
		r.deliver(curr)
	}
}

// This method will be polling while the application is
// alive. The element in the head of the queue will be
// verified every 10 milliseconds and if the element
// changed the delivery method will be called.
func (r *RQueue) poll() {
	defer r.applied.Close()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(5 * time.Millisecond):
			r.verifyAndDeliver()
		}
	}
}

// Will verify if the message can be added into the set.
// The cache will hold messages that already removed and
// cannot be inserted again.
func (r *RQueue) verifyAndInsert(message Message) {
	if _, old := r.applied.Get(string(message.Identifier)); old {
		log.Println("discarding...")
		return
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.set.AddOrUpdate(string(message.Identifier), sortedset.SCORE(message.Timestamp), message)
}

// Implements the Queue interface.
// This method will add the given element into the set,
// if the value already exists it will be updated and if
// there is need the values will be sorted again.
func (r *RQueue) Enqueue(i interface{}) {
	m := i.(Message)
	exists := r.GetIfExists(string(m.Identifier))
	if exists != nil {
		v := exists.(Message)
		// I can only change the message state if the new message contains
		// a timestamp at lest equals to the already present on memory and
		// a state that is currently higher or equal than the previous one.
		if v.Timestamp <= m.Timestamp && v.State <= m.State {
			r.verifyAndInsert(m)
		}
	} else {
		r.verifyAndInsert(m)
	}
}

// Implements the Queue interface.
// Insert a function that will be called when the
// element at the head of the queue changes.
func (r *RQueue) Subscribe(f func(interface{})) {
	r.deliver = f
}

// Implements the Queue interface.
// Get the element on the head of the set.
// This element will probably be the next
// message to be delivered.
func (r *RQueue) Pick() interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	v := r.set.PeekMin()
	if v != nil {
		return v.Value
	}
	return nil
}

// Implements the Queue interface.
func (r *RQueue) Dequeue(i interface{}) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	m := i.(Message)
	value := r.set.Remove(string(m.Identifier))
	if value != nil {
		r.applied.Set(string(m.Identifier), true)
		return value.Value
	}
	return nil
}

// Implements the Queue interface.
func (r *RQueue) GetIfExists(id string) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	v := r.set.GetByKey(id)
	if v != nil {
		return v.Value
	}
	return nil
}

// Implements the Queue interface.
func (r *RQueue) GenericDeliver(i interface{}) {
	message := i.(Message)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	min := r.set.PeekMin()
	max := r.set.PeekMax()

	var messages []Message
	for _, node := range r.set.GetByScoreRange(min.Score(), max.Score(), nil) {
		value := node.Value.(Message)
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
