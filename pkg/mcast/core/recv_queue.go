package core

import (
	"container/heap"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"time"
)

// We should create a structure similar to the sorted set,
// after each change the values must be sorted again.
// The minimum, e.g., the next message to be delivered must
// be on the "head".
// Using this structure, we should not be manually trying to
// retrieve the element on the head, for a given validation
// function the value is returned through a channel, notifying
// that the value is ready.
type RecvQueue interface {
	// Add a new element to the RecvQueue. After this push the
	// elements will be sorted again if a change occurred.
	// This also should be used when just updating a value.
	Push(message interface{})

	// Return all the elements present on the queue at the time
	// of the read. After the elements are returned the actual
	// values can be different.
	All() []types.Message
}

type item struct {
	value types.Message
	index int
}

type priorityQueue struct {
	values []*item

	notification chan <- types.Message

	validation func(message types.Message) bool
}

func NewPriorityQueue(ch chan <- types.Message, validation func(message types.Message) bool) RecvQueue {
	q := &priorityQueue{
		values:       []*item{},
		notification: ch,
		validation:   validation,
	}
	return q
}

func (p priorityQueue) notifyChannel(value types.Message) {
	select {
	case p.notification <- value:
		break
	case <-time.After(100 * time.Millisecond):
		fmt.Printf("Failed notifying about the head change")
		break
	}
}

func (p priorityQueue) Len() int {
	return len(p.values)
}

func (p priorityQueue) Less(i, j int) bool {
	messageA := p.values[i]
	messageB := p.values[j]
	return messageA.value.Cmp(messageB.value) < 0
}

func (p priorityQueue) Swap(i, j int) {
	p.values[i], p.values[j] = p.values[j], p.values[i]
	p.values[i].index = i
	p.values[j].index = j

	if i == 0 && p.validation(p.values[i].value) {
		p.notifyChannel(p.values[i].value)
	}
}

func (p *priorityQueue) Push(x interface{}) {
	defer heap.Init(p)
	msg := x.(types.Message)
	size := len(p.values)
	item := &item{value: msg, index: size}

	if size == 0 {
		p.notifyChannel(msg)
	}

	p.values = append(p.values, item)
}

func (p *priorityQueue) Pop() interface{} {
	defer heap.Init(p)
	old := p.values
	size := len(old)
	item := old[size-1]
	item.index = -1
	p.values = old[0:size-1]
	return item
}

func (p priorityQueue) All() []types.Message {
	var values []types.Message
	for _, value := range p.values {
		values = append(values, value.value)
	}
	return values
}


