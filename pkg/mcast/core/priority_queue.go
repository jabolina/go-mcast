package core

import (
	"container/heap"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

type priorityHeap []types.Message

func (h *priorityHeap) Len() int {
	return len(*h)
}

func (h priorityHeap) Less(i, j int) bool {
	return h[i].HasHigherPriority(h[j])
}

func (h priorityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *priorityHeap) Push(x interface{}) {
	item := x.(types.Message)
	*h = append(*h, item)
}

func (h *priorityHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func (h *priorityHeap) update(message types.Message, index int) {
	curr := (*h)[index]
	if curr.Updated(message) {
		(*h)[index] = message
		heap.Fix(h, index)
	}
}

// A priority queue that uses a heap for ordering elements.
type PriorityQueue struct {
	// Synchronize operations on the Message slice.
	mutex *sync.Mutex

	// The elements present on the queue.
	values priorityHeap

	// A channel for notification about changes on the head element.
	notification chan<- types.Message

	// A function to verify if the given element can be notified.
	validation func(message types.Message) bool
}

func NewPriorityQueue(ch chan<- types.Message, validation func(message types.Message) bool) types.ReceivedQueue {
	return &PriorityQueue{
		mutex:        &sync.Mutex{},
		values:       priorityHeap{},
		notification: ch,
		validation:   validation,
	}
}

// Sends a notification back through the channel when the head value
// changes. See that this method can issue the same object multiple
// times. The listener is responsible for handling the duplicated values.
func (p *PriorityQueue) sendNotification() {
	msg := p.values[0]
	p.notification <- msg
}

// Get a item index by the given UID.
// This method should be called while holding the mutex.
func (p *PriorityQueue) getIndexByUid(uid types.UID) int {
	for index, value := range p.values {
		if value.Identifier == uid {
			return index
		}
	}
	return -1
}

func (p *PriorityQueue) Push(message types.Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.values.Len() > 0 {
		// If the head already contains an element.
		// We must verify it with the value when the function returns.
		headStart := p.values[0]
		defer func() {
			headCurrent := p.values[0]
			if headStart.Diff(headCurrent) && p.validation(headCurrent) {
				p.sendNotification()
			}
		}()
	} else {
		defer func() {
			head := p.values[0]
			// We know the head was empty and now has a value, so
			// it definitely changed, now only verify if the value
			// can notify.
			if p.validation(head) {
				p.sendNotification()
			}
		}()
	}

	index := p.getIndexByUid(message.Identifier)
	if index < 0 {
		heap.Push(&p.values, message)
	} else {
		p.values.update(message, index)
	}
}

func (p *PriorityQueue) Pop() interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.values.Len() == 0 {
		return nil
	}

	defer func() {
		if p.values.Len() > 0 {
			// The Pop method will remove the head element, so
			// if we still have elements the head definitely changed,
			// we only need to verify if the value can notify.
			headCurrent := p.values[0]
			if p.validation(headCurrent) {
				p.sendNotification()
			}
		}
	}()

	return heap.Pop(&p.values)
}

func (p *PriorityQueue) Remove(uid types.UID) interface{} {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	index := p.getIndexByUid(uid)
	if index < 0 {
		return nil
	}

	// We are not sure if the removed element is the
	// head, so we will verify the old head with the
	// current after the function returns.
	headStart := p.values[0]
	defer func() {
		if p.values.Len() > 0 {
			headCurrent := p.values[0]
			if headStart.Diff(headCurrent) && p.validation(headCurrent) {
				p.sendNotification()
			}
		}
	}()

	return heap.Remove(&p.values, index)
}

func (p *PriorityQueue) Values() []types.Message {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	var messages []types.Message
	messages = append(messages, p.values...)
	return messages
}

func (p *PriorityQueue) GetByKey(uid types.UID) (types.Message, bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	index := p.getIndexByUid(uid)
	if index < 0 {
		return types.Message{}, false
	}
	return p.values[index], true
}
