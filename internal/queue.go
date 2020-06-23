package internal

import "sync"

// A Queue interface.
type Queue interface {
	// Add a new item.
	Enqueue(interface{})

	// Pick the first item from the queue.
	Pick() interface{}

	// Remove the given item from the queue.
	Dequeue(interface{}) interface{}

	// Create a snapshot of the current values
	// on the queue.
	Snapshot() []interface{}
}

// Implements the queue interface.
// This will be used by a single peer to hold information
// about processing messages.
// Event though is a queue, since all messages will contains
// a unique identifier, is easier to implement the "queue"
// using a map to hold the messages.
type RQueue struct {
	// Synchronization for operations.
	mutex *sync.Mutex

	// Actual message values.
	values map[UID]Message
}

func NewQueue() Queue {
	return &RQueue{
		mutex:  &sync.Mutex{},
		values: make(map[UID]Message),
	}
}

// Implements the Queue interface.
func (r *RQueue) Enqueue(i interface{}) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	switch m := i.(type) {
	case Message:
		r.values[m.Identifier] = m
	case *Message:
		r.values[m.Identifier] = *m
	default:
		panic("value is not a Message")
	}
}

// Implements the Queue interface.
func (r *RQueue) Pick() interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, message := range r.values {
		return message
	}
	return nil
}

// Implements the Queue interface.
func (r *RQueue) Dequeue(i interface{}) interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	switch m := i.(type) {
	case Message:
		v, ok := r.values[m.Identifier]
		if !ok {
			return nil
		}
		return v
	case *Message:
		v, ok := r.values[m.Identifier]
		if !ok {
			return nil
		}
		return v
	default:
		panic("type is not a Message")
	}
}

// Implements the Queue interface.
func (r *RQueue) Snapshot() []interface{} {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	var snapshot []interface{}
	for _, message := range r.values {
		snapshot = append(snapshot, message)
	}
	return snapshot
}
