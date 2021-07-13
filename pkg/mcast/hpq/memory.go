package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
	"sync"
)

// Memory is the interface responsible for interacting with
// the received messages.
type Memory interface {
	io.Closer

	// Acceptable conflict if the given interface is eligible to be
	// added to the queue.
	Acceptable(types.Message) bool

	// Append enqueue a new item and returns true if a change
	// occurred and false otherwise.
	Append(types.Message) bool

	// Remove the given item from the queue.
	Remove(types.Message)

	// Exists is a function to simulate the mathematical exists.
	// Can be used to conflict if exists an element that the predicate
	// is true.
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

type PeerQueueManager struct {
	ctx       context.Context
	conflict  func(types.Message, types.Message) bool
	eden      Eden
	purgatory Purgatory
	notify    chan<- ElementNotification
	flag      *helper.Flag
	mutex     *sync.Mutex
}

func NewReceivedQueue(ctx context.Context, notify chan<- ElementNotification, verify func(types.Message, types.Message) bool) Memory {
	p := &PeerQueueManager{
		ctx:       ctx,
		conflict:  verify,
		purgatory: NewPurgatory(),
		notify:    notify,
		flag:      &helper.Flag{},
		mutex:     &sync.Mutex{},
	}
	p.eden = NewEden(ctx, p.applyOnDeliver)
	return p
}

func (p *PeerQueueManager) notifyElement(en ElementNotification) {
	if p.flag.IsActive() {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		select {
		case <-p.ctx.Done():
		case p.notify <- en:
		}
	}
}

func (p *PeerQueueManager) applyOnDeliver(notification ElementNotification) bool {
	if p.purgatory.Set(string(notification.Value.(types.Message).Identifier)) {
		p.notifyElement(notification)
		return true
	}
	return false
}

func (p *PeerQueueManager) Close() error {
	if p.flag.Inactivate() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		close(p.notify)
	}
	return nil
}

func (p *PeerQueueManager) Acceptable(message types.Message) bool {
	return !p.purgatory.Contains(string(message.Identifier))
}

func (p *PeerQueueManager) Append(message types.Message) bool {
	if !p.Acceptable(message) {
		return false
	}
	return p.eden.Enqueue(message)
}

func (p *PeerQueueManager) Remove(message types.Message) {
	p.eden.Dequeue(message)
}

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

func (p *PeerQueueManager) GenericDeliver(message types.Message) {
	if !p.Acceptable(message) {
		return
	}

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

		// This will copy the slice at the time of read.
		// This method does not guarantee that we have the
		// latest object version, the elements can change after
		// we read the slice.
		// Since the elements that will be delivered here could
		// be delivered at any order, this should not be a problem.
		for _, value := range current {
			if value.State == types.S3 && !existsConflict(value) {
				messages = append(messages, value)
			}
		}

		for _, m := range messages {
			// If the Message do not conflict with any other Message
			// then it can be delivered directly.
			if p.purgatory.Set(string(m.Identifier)) {
				p.notifyElement(ElementNotification{
					Value:   m,
					OnApply: true,
				})
			}
		}
	})
}
