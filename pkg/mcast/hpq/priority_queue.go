package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

// IPriorityQueue implements a priority queue.
// The minimum, e.g., the next Message to be delivered must
// be on the "head".
// Using this structure, we should not be manually trying to
// retrieve the element on the head, for a given validation
// function the Value is returned through a channel, notifying
// that the Value is ready.
type IPriorityQueue interface {
	// Push add a new element to the RecvQueue. After this push the
	// elements will be sorted again if a change occurred.
	// This also should be used when just updating a Value.
	Push(message types.Message)

	// Pop removes the next element that is ready to be delivered from
	// the queue. After the element is removed the heap algorithm
	// will be executed again.
	Pop() interface{}

	// Remove the Message that contains the given identifier.
	// After the item is removed the heap sorted again.
	Remove(uid types.UID) interface{}

	// Get tries to return the element associated with the
	// given uid.
	Get(uid types.UID) interface{}

	// Values return all the elements present on the queue at the time
	// of the read. After the elements are returned the actual
	// values can be different.
	Values() []types.Message
}

type MessageWrapper struct {
	Message types.Message
}

// PriorityQueue uses a heap for ordering elements.
type PriorityQueue struct {
	// Synchronize operations on the Message slice.
	mutex *sync.RWMutex

	ctx context.Context

	// Heap that hold all entries for the priority queue.
	values Heap

	// A channel for notification about changes on the head element.
	notification chan<- types.Message

	// A function to verify if the given element can be notified.
	validation func(message types.Message) bool
}

func NewPriorityQueue(ctx context.Context, ch chan<- types.Message, validation func(message types.Message) bool) IPriorityQueue {
	return &PriorityQueue{
		mutex:        &sync.RWMutex{},
		ctx:          ctx,
		values:       NewHeap(),
		notification: ch,
		validation:   validation,
	}
}

// Sends a notification back through the channel when the head Value
// changes. See that this method can issue the same object multiple
// times. The listener is responsible for handling the duplicated values.
func (p *PriorityQueue) sendNotification() {
	msg := p.values.Peek()
	select {
	case <-p.ctx.Done():
	case p.notification <- unwrap(msg):
	}
}

func (p *PriorityQueue) Push(message types.Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.values.IsEmpty() {
		// If the head already contains an element.
		// We must verify it with the Value when the function returns.
		headStart := unwrap(p.values.Peek())
		defer func() {
			headCurrent := unwrap(p.values.Peek())
			if headStart.Diff(headCurrent) && p.validation(headCurrent) {
				p.sendNotification()
			}
		}()
	} else {
		defer func() {
			head := unwrap(p.values.Peek())
			// We know the head was empty and now has a Value, so
			// it definitely changed, now only verify if the Value
			// can notify.
			if p.validation(head) {
				p.sendNotification()
			}
		}()
	}

	mw := MessageWrapper{Message: message}
	p.values.Insert(mw)
}

func (p *PriorityQueue) Pop() interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	defer func() {
		if !p.values.IsEmpty() {
			// The Pop method will remove the head element, so
			// if we still have elements the head definitely changed,
			// we only need to verify if the Value can notify.
			headCurrent := unwrap(p.values.Peek())
			if p.validation(headCurrent) {
				p.sendNotification()
			}
		}
	}()

	return unwrapOrNil(p.values.Pop())
}

func (p *PriorityQueue) Get(uid types.UID) interface{} {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	mw := MessageWrapper{
		Message: types.Message{Identifier: uid},
	}
	return unwrapOrNil(p.values.Get(mw))
}

func (p *PriorityQueue) Remove(uid types.UID) interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// We are not sure if the removed element is the
	// head, so we will verify the old head with the
	// current after the function returns.
	if !p.values.IsEmpty() {
		headStart := unwrap(p.values.Peek())
		defer func() {
			if !p.values.IsEmpty() {
				headCurrent := unwrap(p.values.Peek())
				if headStart.Diff(headCurrent) && p.validation(headCurrent) {
					p.sendNotification()
				}
			}
		}()
	}

	mw := MessageWrapper{Message: types.Message{Identifier: uid}}
	return unwrapOrNil(p.values.Remove(mw))
}

func (p *PriorityQueue) Values() []types.Message {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	curr := p.values.Values()

	var messages []types.Message
	for _, msg := range curr {
		messages = append(messages, unwrap(msg))
	}

	return messages
}

func (m MessageWrapper) Id() interface{} {
	return m.Message.Identifier
}

func (m MessageWrapper) Less(item HeapItem) bool {
	other := item.(MessageWrapper)
	return m.Message.HasHigherPriority(other.Message)
}

func unwrapOrNil(i interface{}) interface{} {
	if i == nil {
		return nil
	}
	return unwrap(i)
}

func unwrap(i interface{}) types.Message {
	return i.(MessageWrapper).Message
}
