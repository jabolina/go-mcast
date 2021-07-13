package hpq

import (
	"context"
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
	Push(QueueElement)

	// Pop removes the next element that is ready to be delivered from
	// the queue. After the element is removed the heap algorithm
	// will be executed again.
	Pop() interface{}

	// Remove the Message that contains the given identifier.
	// After the item is removed the heap sorted again.
	Remove(QueueElement) interface{}

	// Get tries to return the element associated with the
	// given uid.
	Get(QueueElement) interface{}

	// Execute will lock the structure and executes the given function.
	Execute(func([]QueueElement))
}

type QueueElement interface {
	HeapItem

	Diff(QueueElement) bool
}

// PriorityQueue uses a heap for ordering elements.
type PriorityQueue struct {
	// Synchronize operations on the Message slice.
	mutex *sync.RWMutex

	ctx context.Context

	// Heap that hold all entries for the priority queue.
	values Heap

	// A channel for notification about changes on the head element.
	notification chan<- QueueElement

	// A function to verify if the given element can be notified.
	validation func(QueueElement) bool
}

func NewPriorityQueue(ctx context.Context, ch chan<- QueueElement, validation func(QueueElement) bool) IPriorityQueue {
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
	msg := p.values.Peek().(QueueElement)
	select {
	case <-p.ctx.Done():
	case p.notification <- msg:
	}
}

func (p *PriorityQueue) Push(element QueueElement) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.values.IsEmpty() {
		// If the head already contains an element.
		// We must verify it with the Value when the function returns.
		headStart := p.values.Peek().(QueueElement)
		defer func() {
			headCurrent := p.values.Peek().(QueueElement)
			if headStart.Diff(headCurrent) && p.validation(headCurrent) {
				p.sendNotification()
			}
		}()
	} else {
		defer func() {
			head := p.values.Peek().(QueueElement)
			// We know the head was empty and now has a Value, so
			// it definitely changed, now only conflict if the Value
			// can notify.
			if p.validation(head) {
				p.sendNotification()
			}
		}()
	}

	p.values.Insert(element)
}

func (p *PriorityQueue) Pop() interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	defer func() {
		if !p.values.IsEmpty() {
			// The Pop method will remove the head element, so
			// if we still have elements the head definitely changed,
			// we only need to verify if the Value can notify.
			headCurrent := p.values.Peek().(QueueElement)
			if p.validation(headCurrent) {
				p.sendNotification()
			}
		}
	}()

	return p.values.Pop()
}

func (p *PriorityQueue) Get(element QueueElement) interface{} {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.values.Get(element)
}

func (p *PriorityQueue) Remove(element QueueElement) interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// We are not sure if the removed element is the
	// head, so we will verify the old head with the
	// current after the function returns.
	if !p.values.IsEmpty() {
		headStart := p.values.Peek().(QueueElement)
		defer func() {
			if !p.values.IsEmpty() {
				headCurrent := p.values.Peek().(QueueElement)
				if headStart.Diff(headCurrent) && p.validation(headCurrent) {
					p.sendNotification()
				}
			}
		}()
	}

	return p.values.Remove(element)
}

// Execute ensure that the queue does not change while we apply some
// function with the values on the priority queue. If the values are copied
// and then returned there is no guarantee that the used values are the actual values.
func (p *PriorityQueue) Execute(f func([]QueueElement)) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	curr := p.values.Values()

	var messages []QueueElement
	for _, e := range curr {
		messages = append(messages, e.(QueueElement))
	}

	f(messages)
}
