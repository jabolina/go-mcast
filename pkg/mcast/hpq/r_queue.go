package hpq

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
	"sync"
)

// IRQueue is the interface responsible for interacting with
// the received messages.
type IRQueue interface {
	io.Closer

	// Acceptable verify if the given interface is eligible to be
	// added to the queue.
	Acceptable(types.Message) bool

	// Append enqueue a new item and returns true if a change
	// occurred and false otherwise.
	Append(types.Message) bool

	// Remove the given item from the queue.
	Remove(types.Message)

	// GenericDeliver is what turns the protocol into its generic
	// form, where not all messages are sorted.
	// This will verify if the given Message conflict with other
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
	conflict  types.ConflictRelationship
	eden      Eden
	purgatory Purgatory
	notify    chan<- ElementNotification
	flag      *helper.Flag
	mutex     *sync.Mutex
}

func NewReceivedQueue(ctx context.Context, notify chan<- ElementNotification, conflict types.ConflictRelationship) IRQueue {
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

func (p *PeerQueueManager) GenericDeliver(message types.Message) {
	if !p.Acceptable(message) {
		return
	}

	p.eden.Apply(func(current []types.Message) {
		var messages []types.Message
		// This will copy the slice at the time of read.
		// This method does not guarantee that we have the
		// latest object version, the elements can change after
		// we read the slice.
		// Since the elements that will be delivered here could
		// be delivered at any order, this should not be a problem.
		for _, value := range current {
			if value.Identifier != message.Identifier {
				messages = append(messages, value)
			}
		}

		// If the Message do not conflict with any other Message
		// then it can be delivered directly.
		if !p.conflict.Conflict(message, messages) && p.purgatory.Set(string(message.Identifier)) {
			p.notifyElement(ElementNotification{
				Value:   message,
				OnApply: true,
			})
		}
	})
}
