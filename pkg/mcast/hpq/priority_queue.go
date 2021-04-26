package hpq

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

// IPriorityQueue implements a priority queue.
// The minimum, e.g., the next message to be delivered must
// be on the "head".
// Using this structure, we should not be manually trying to
// retrieve the element on the head, for a given validation
// function the value is returned through a channel, notifying
// that the value is ready.
type IPriorityQueue interface {
	// Push add a new element to the RecvQueue. After this push the
	// elements will be sorted again if a change occurred.
	// This also should be used when just updating a value.
	Push(message types.Message)

	// Pop removes the next element that is ready to be delivered from
	// the queue. After the element is removed the heap algorithm
	// will be executed again.
	Pop() interface{}

	// Remove the message that contains the given identifier.
	// After the item is removed the heap sorted again.
	Remove(uid types.UID) interface{}

	// Values return all the elements present on the queue at the time
	// of the read. After the elements are returned the actual
	// values can be different.
	Values() []types.Message
}

// PriorityQueue uses a heap for ordering elements.
type PriorityQueue struct {
	// Synchronize operations on the Message slice.
	mutex *sync.RWMutex

	// Heap that hold all entries for the priority queue.
	values Heap

	// A channel for notification about changes on the head element.
	notification chan<- types.Message

	// A function to verify if the given element can be notified.
	validation func(message types.Message) bool
}

func NewPriorityQueue(ch chan<- types.Message, validation func(message types.Message) bool) IPriorityQueue {
	return &PriorityQueue{
		mutex:        &sync.RWMutex{},
		values:       NewHeap(),
		notification: ch,
		validation:   validation,
	}
}

// Sends a notification back through the channel when the head value
// changes. See that this method can issue the same object multiple
// times. The listener is responsible for handling the duplicated values.
func (p *PriorityQueue) sendNotification() {
	msg := p.values.Peek()
	p.notification <- msg.(types.Message)
}

func (p *PriorityQueue) Push(message types.Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.values.IsEmpty() {
		// If the head already contains an element.
		// We must verify it with the value when the function returns.
		headStart := p.values.Peek().(types.Message)
		defer func() {
			headCurrent := p.values.Peek().(types.Message)
			if headStart.Diff(headCurrent) && p.validation(headCurrent) {
				p.sendNotification()
			}
		}()
	} else {
		defer func() {
			head := p.values.Peek().(types.Message)
			// We know the head was empty and now has a value, so
			// it definitely changed, now only verify if the value
			// can notify.
			if p.validation(head) {
				p.sendNotification()
			}
		}()
	}

	p.values.Insert(message)
}

func (p *PriorityQueue) Pop() interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	defer func() {
		if !p.values.IsEmpty() {
			// The Pop method will remove the head element, so
			// if we still have elements the head definitely changed,
			// we only need to verify if the value can notify.
			headCurrent := p.values.Peek().(types.Message)
			if p.validation(headCurrent) {
				p.sendNotification()
			}
		}
	}()

	return p.values.Pop()
}

func (p *PriorityQueue) Remove(uid types.UID) interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// We are not sure if the removed element is the
	// head, so we will verify the old head with the
	// current after the function returns.
	if !p.values.IsEmpty() {
		headStart := p.values.Peek().(types.Message)
		defer func() {
			if !p.values.IsEmpty() {
				headCurrent := p.values.Peek().(types.Message)
				if headStart.Diff(headCurrent) && p.validation(headCurrent) {
					p.sendNotification()
				}
			}
		}()
	}

	return p.values.Remove(types.Message{Identifier: uid})
}

func (p *PriorityQueue) Values() []types.Message {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	curr := p.values.Values()

	var messages []types.Message
	for _, msg := range curr {
		messages = append(messages, msg.(types.Message))
	}

	return messages
}
