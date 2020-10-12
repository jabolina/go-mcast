package core

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sort"
	"sync"
	"time"
)

// This interface is used as a replacement for the heap.Interface.
// The default golang interface executes the Swap method between the
// head and the last element when a Pop is called, this leads to an
// undesired behaviour where the head changes for the wrong reasons.
//
// The minimum, e.g., the next message to be delivered must
// be on the "head".
// Using this structure, we should not be manually trying to
// retrieve the element on the head, for a given validation
// function the value is returned through a channel, notifying
// that the value is ready.
type RecvQueue interface {
	sort.Interface

	// Add a new element to the RecvQueue. After this push the
	// elements will be sorted again if a change occurred.
	// This also should be used when just updating a value.
	Push(x types.Message)

	// Remove the next element that is ready to be delivered from
	// the queue. After the element is removed the heap algorithm
	// will be executed again.
	Pop() *types.Message

	// Remove the message that contains the given identifier.
	// After the item is removed the heap sorted again.
	Remove(uid types.UID)

	// Return all the elements present on the queue at the time
	// of the read. After the elements are returned the actual
	// values can be different.
	Values() []types.Message

	// Get the Message element by the given UID. If the value is
	// not present returns nil.
	GetByKey(uid types.UID) *types.Message
}

// A priority queue that uses a heap for ordering elements.
type PriorityQueue struct {
	// Synchronize operations on the Message slice.
	mutex *sync.Mutex

	// The elements present on the queue.
	values []types.Message

	// A channel for notification about changes on the head element.
	notification chan<- types.Message

	// A function to verify if the given element can be notified.
	validation func(message types.Message) bool
}

func NewPriorityQueue(ch chan<- types.Message, validation func(message types.Message) bool) RecvQueue {
	q := &PriorityQueue{
		mutex:        &sync.Mutex{},
		values:       []types.Message{},
		notification: ch,
		validation:   validation,
	}
	return q
}

// Get a item index by the given UID.
// This method should be called while holding the mutex.
func (p PriorityQueue) getIndexByUid(uid types.UID) int {
	for index, value := range p.values {
		if value.Identifier == uid {
			return index
		}
	}
	return -1
}

// Sends a notification back through the channel when the head value
// changes. See that this method can issue the same object multiple
// times. The listener is responsible for handling the duplicated values.
func (p *PriorityQueue) sendNotification() {
	if p.Len() > 0 {
		select {
		case p.notification <- p.values[0]:
			break
		// TODO: configure timeout
		case <-time.After(100 * time.Millisecond):
			break
		}
	}
}

// Remove the last element from the values slice and return it.
// This method does not verify for the slice length,
// is the caller responsibility to call it when a value exists.
func (p *PriorityQueue) remove() *types.Message {
	old := p.values
	n := len(old)
	item := old[n-1]
	(*p).values = old[0 : n-1]
	return &item
}

// Heap up method, see heap.up.
func (p *PriorityQueue) up(j int) {
	for {
		parent := (j - 1) / 2
		if parent == j || !p.Less(j, parent) {
			break
		}
		p.Swap(parent, j)
		j = parent
	}
}

// Heap down method, see heap.down.
func (p *PriorityQueue) down(start, n int) bool {
	root := start
	for {
		tmpLeft := 2*root + 1
		if tmpLeft >= n || tmpLeft < 0 {
			break
		}
		left := tmpLeft
		if right := tmpLeft + 1; right < n && p.Less(right, tmpLeft) {
			left = right
		}

		if !p.Less(left, root) {
			break
		}
		p.Swap(root, left)
		root = left
	}
	return root > start
}

// Implements the sort.Interface.
// Return the length of the queue.
func (p *PriorityQueue) Len() int {
	return len(p.values)
}

// Implements the sort.Interface.
// Verify if the value at index i < value at index j.
func (p *PriorityQueue) Less(i, j int) bool {
	return p.values[i].Cmp(p.values[j]) < 0
}

// Implements the sort.Interface.
// Swap the items for the given indexes.
func (p *PriorityQueue) Swap(i, j int) {
	p.values[i], p.values[j] = p.values[j], p.values[i]
}

// Implements the RecvQueue interface.
// Add or update an element for the PriorityQueue.
// If the value already exists it will be replace by the given
// value, otherwise it will be added.
// After that, if the head changed, a notification will be sent.
// In both cases, the heap algorithm will be executed prior to
// verifying the head for changes.
func (p *PriorityQueue) Push(x types.Message) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.Len() > 0 {
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

	index := p.getIndexByUid(x.Identifier)
	if index < 0 {
		p.values = append(p.values, x)
		p.up(p.Len() - 1)
	} else {
		p.values[index] = x
		if !p.down(index, p.Len()) {
			p.up(index)
		}
	}
}

// Implements the RecvQueue interface.
// Read and remove the element at the head of the queue.
// If no value exists, return nil.
// If the value at the head of the queue changed, a notification
// will be sent.
func (p *PriorityQueue) Pop() *types.Message {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.Len() == 0 {
		return nil
	}

	defer func() {
		if p.Len() > 0 {
			// The Pop method will remove the head element, so
			// if we still have elements the head definitely changed,
			// we only need to verify if the value can notify.
			headCurrent := p.values[0]
			if p.validation(headCurrent) {
				p.sendNotification()
			}
		}
	}()

	n := p.Len() - 1
	p.values[0], p.values[n] = p.values[n], p.values[0]
	p.down(0, n)
	return p.remove()
}

// Implements the RecvQueue interface.
// Removes the element associated with the given UID.
// If the element does not exists, do nothing. If the
// element removal changes the head of the queue, a notification
// will be sent.
func (p *PriorityQueue) Remove(uid types.UID) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	index := p.getIndexByUid(uid)
	if index < 0 {
		return
	}

	// We are not sure if the removed element is the
	// head, so we will verify the old head with the
	// current after the function returns.
	headStart := p.values[0]
	defer func() {
		if p.Len() > 0 {
			headCurrent := p.values[0]
			if headStart.Diff(headCurrent) && p.validation(headCurrent) {
				p.sendNotification()
			}
		}
	}()

	n := p.Len() - 1
	if n != index {
		p.Swap(index, n)
		if !p.down(index, n) {
			p.up(index)
		}
	}
	p.remove()
}

// Implements the RecvQueue interface.
func (p *PriorityQueue) Values() []types.Message {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.values
}

// Implements the RecvQueue interface.
func (p *PriorityQueue) GetByKey(uid types.UID) *types.Message {
	index := p.getIndexByUid(uid)
	if index < 0 {
		return nil
	}
	return &p.values[index]
}
