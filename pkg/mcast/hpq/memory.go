package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
	"sync"
)

// Memory is the interface responsible for interacting with
// the received messages. All messages that are being processes are
// kept inside the Memory and can only be accessed through the available
// methods in this interface. This structure is also responsible for holding
// information about messages that already delivered previously.
type Memory interface {
	io.Closer

	// Append enqueue a new item and returns true if a change
	// occurred and false otherwise.
	Append(types.Message) bool

	// Remove the given item from the queue.
	Remove(types.Message)

	// Exists is a function to simulate the mathematical exists.
	// Can be used to verify if exists an element where the predicate
	// returns true.
	Exists(func(types.Message) bool) bool

	// GenericDeliver is what turns the protocol into its generic
	// form, where not all messages are sorted.
	// This will conflict if the given Message conflict with other
	// messages and will delivery if possible.
	//
	// A Message will only be able to be delivered if is on state
	// S3 and do not conflict with any other messages.
	GenericDeliver(message types.Message)
}

type ElementNotification struct {
	Value   interface{}
	OnApply bool
}

// PeerQueueManager is the concrete implementation for the Memory interface.
// Since it will be responsible for handling messages that will proceed and
// messages that already delivered, they are kept in different areas. Messages
// that are currently processed by the algorithm resides in the area called
// `eden` and messages that already delivered resides in the area called `purgatory`.
//
// This structure also notify that client when the head of the queue changes,
// and fulfill the requirements. The queue is in the eden area, using a priority
// queue implementation that uses a Fibonacci heap to keep the element that is
// most likely to be delivered next in the head of the queue. The element that is
// most likely is the element with the smallest timestamp, to break tie with same
// timestamp that message unique identifier is used. The client notification about
// head change is triggered when the message is on state `types.S3`, meaning that the
// message is ready to be delivered, since contains the smallest timestamp and already
// on the final state.
type PeerQueueManager struct {
	// The ctx argument is the parent context, to verify if the application is closed.
	ctx context.Context

	// The conflict function receive two messages as arguments and returns if the
	// two messages can commute or not.
	conflict func(types.Message, types.Message) bool

	// The eden structure holds messages that are currently being processed by
	// the protocol.
	eden Eden

	// The purgatory structure holds information about messages that were
	// already delivered by the protocol.
	purgatory Purgatory

	// The notify channel is used to send notification when a message is ready
	// to be delivered by the client.
	notify chan<- ElementNotification

	// The flag is used internally to handle the Memory shutdown.
	flag *helper.Flag

	// The mutex is used internally only.
	mutex *sync.Mutex
}

// NewReceivedQueue creates a new Memory structure with the given arguments.
// The ctx is the parent context, notify is the channel the client give so the Memory
// can notify when the head element change and is ready to deliver, the conflict
// argument is a function that receive two messages as arguments and returns if
// the two messages can commute or not.
func NewReceivedQueue(ctx context.Context, notify chan<- ElementNotification, conflict func(types.Message, types.Message) bool) Memory {
	p := &PeerQueueManager{
		ctx:       ctx,
		conflict:  conflict,
		purgatory: NewPurgatory(),
		notify:    notify,
		flag:      &helper.Flag{},
		mutex:     &sync.Mutex{},
	}
	p.eden = NewEden(ctx, p.applyOnDeliver)
	return p
}

// The notifyElement will verify if it is possible to notify the Memory client
// about an element to be delivered. The element will not notify if the Memory
// flag is inactive or if the parent context is already done.
func (p *PeerQueueManager) notifyElement(en ElementNotification) {
	if p.flag.IsActive() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		select {
		case <-p.ctx.Done():
		// This can panic if the Close method executes first and close
		// the notify channel.
		case p.notify <- en:
		}
	}
}

// The applyOnDeliver method receives a notification from the underlying
// queue structure and try to notify the Memory client about a new element
// to be delivered. When verifying if the message can be delivered, it will also
// add the element in the purgatory region, since it is ready to be delivered,
// will be removed from the head of the queue and will not proceed in the protocol.
func (p *PeerQueueManager) applyOnDeliver(notification ElementNotification) bool {
	if p.purgatory.Set(string(notification.Value.(types.Message).Identifier)) {
		p.notifyElement(notification)
		return true
	}
	return false
}

// Close implements the io.Closer interface.
// This method will define the flag as inactive and will close the notify
// channel. This can be called multiple times.
func (p *PeerQueueManager) Close() error {
	if p.flag.Inactivate() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		close(p.notify)
	}
	return nil
}

// Append implements the Memory interface.
// The method will insert the given message in the queue that resides in the
// eden region. The message will only be inserted if if was not delivered
// previously.
func (p *PeerQueueManager) Append(message types.Message) bool {
	return p.eden.Enqueue(message)
}

// Remove implements the Memory interface.
// This method will remove the given message from the priority queue that
// resides in the eden region. This will **not** add the message in the
// purgatory region.
func (p *PeerQueueManager) Remove(message types.Message) {
	p.eden.Dequeue(message)
}

// Exists implements the Memory interface.
// This method simulates the mathematical exists ∃. This will verify
// for all elements that are present on the queue, will return at the first
// element where the given predicate returns true.
func (p *PeerQueueManager) Exists(f func(types.Message) bool) bool {
	var response bool
	p.eden.Apply(func(messages []types.Message) {
		for _, message := range messages {
			if f(message) {
				response = true
				return
			}
		}
	})
	return response
}

// GenericDeliver implements the Memory interface.
// This method is responsible to verify if a message can be delivered using
// the generalized property. Besides finding if the given message can be generic
// delivered, it will also verify if there are another messages that can also be
// generic delivered.
func (p *PeerQueueManager) GenericDeliver(message types.Message) {
	p.eden.Apply(func(current []types.Message) {
		var messages []types.Message

		existsConflict := func(m types.Message) bool {
			for _, n := range current {
				if m.Identifier != n.Identifier && p.conflict(m, n) {
					return true
				}
			}
			return false
		}

		// If the current given message does not conflict with any other,
		// it can also be added to be delivered.
		if !existsConflict(message) {
			messages = append(messages, message)
		}

		// Verify if each message does not conflict with any other message
		// present in the queue. If a message does not conflict with any
		// other message and is on state S3, then it is ready to be delivered.
		// This loop is O(n**2) on the queue size.
		for _, value := range current {
			if value.State == types.S3 && !existsConflict(value) {
				messages = append(messages, value)
			}
		}

		for _, m := range messages {
			// Messages on the `messages` slice does not conflict with
			// any other message, so they can be delivered using the generalized
			// property. To ensure a message is not delivered more than once,
			// the message identifier is verified in the purgatory.
			if p.purgatory.Set(string(m.Identifier)) {
				p.notifyElement(ElementNotification{
					Value:   m,
					OnApply: true,
				})
			}
		}
	})
}
