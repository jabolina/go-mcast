package internal

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Interface to deliver messages.
type Deliverable interface {
	// Verify if exists messages that are ready to be
	// delivered.
	Verify([]Message) (bool, []Message)

	// Deliver ready message from the slice of messages.
	Deliver([]Message)

	// Commit the given message on the state machine.
	Commit(message Message)

	// Delivers a channel to listen for commit updates.
	Listen() <-chan Response
}

// A struct that is able to deliver message from the protocol.
// The messages will be committed on the peer state machine
// and a notification will be generated,
type Deliver struct {
	deliver chan []Message

	// Parent context of the delivery.
	// The parent who instantiate the delivery is the peer that
	// relies inside a partition, so for each peer will exists a
	// deliver instance.
	// When the peer is shutdown, also will be shutdown the deliver.
	ctx context.Context

	// Channel for commit notifications.
	onCommit chan Response

	// Conflict relationship to order the messages.
	conflict ConflictRelationship

	// The peer state machine.
	sm StateMachine

	// Deliver logger.
	log Logger
}

// Creates a new instance of the Deliverable interface.
func NewDeliver(invoker *Invoker, ctx context.Context, log Logger, conflict ConflictRelationship, storage Storage) (Deliverable, error) {
	sm := NewStateMachine(storage)
	if err := sm.Restore(); err != nil {
		return nil, err
	}
	d := &Deliver{
		ctx:      ctx,
		deliver:  make(chan []Message),
		onCommit: make(chan Response),
		conflict: conflict,
		sm:       sm,
		log:      log,
	}
	invoker.invoke(d.execute)
	return d, nil
}

func (d Deliver) Deliver(messages []Message) {
	d.deliver <- messages
}

// Start the deliver sequence for the given message slice.
// Each message m with state equals to S3 already has its final
// timestamp and is ready to be delivered on the right order. Firstly,
// for each message m that do not conflict with any other message with states
// S0, S1, and S2, m can be delivered.
//
// Secondly, the algorithm search for the message m with the smaller timestamp
// m.tsf between all other messages, if m is on state S3 and do not conflict with
// any other message on the same timestamp, m can be delivered.
//
// At last, when exists more than one message with state S3 and the same timestamp
// conflicting, the algorithm has to choose a deterministic way to sort the message
// and deliver on the sorted order, for example, sorting by the UID as string.
func (d Deliver) Verify(messages []Message) (bool, []Message) {
	var otherMessages []Message
	var ready []Message
	for _, message := range messages {
		if message.State == S3 {
			ready = append(ready, message)
			continue
		}

		if canHandleState(message) {
			otherMessages = append(otherMessages, message)
		}
	}

	if len(ready) == 0 {
		return false, ready
	}

	// Since we also need to verify the timestamp of other messages
	// that are not ready yet, it is easier to just sort the array.
	sortMessagesSlice(otherMessages)

	// First the messages that are ready to be delivered will be sorted.
	// The messages will be sorted by the final timestamp, if exists messages
	// with the same timestamp then the messages will be sorted by the given
	// unique identifier, sorting by a string comparison.
	sortMessagesSlice(ready)

	var final []Message
	for _, m := range ready {
		// This is the first verification and attempt to deliver messages
		// that are already on state S3 and do not conflicts with other messages
		// that relies on state {S0, S1, S2}.
		//
		// Exists a messages m, m.State = S3 and for all message m',
		// m'.State in {S0, S1, S3} -> m do not conflict with m'.
		if !d.conflict.Conflict(m, otherMessages) {
			final = append(final, m)
			continue
		}

		if len(otherMessages) > 0 {
			first := otherMessages[0]
			// If the message conflict with other, it can be committed if the message
			// on the state S3 has the smallest timestamp when compared with the
			// messages on other states.
			//
			// Exists a messages m, m.State = S3 and for all message m',
			// m'.State in {S0, S1, S3} -> m.Timestamp < m'.Timestamp.
			if m.Timestamp < first.Timestamp {
				final = append(final, m)
			}
		} else {
			final = append(final, m)
		}
	}
	return len(final) > 0, final
}

func (d Deliver) execute() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case messages := <-d.deliver:
			// Message was added to the list on the the same order they must be committed
			// so now commit one a time.
			for _, message := range messages {
				d.Commit(message)
			}
		}
	}
}

// Commit the res for the give message m that is being processed.
// After the commit a response is sent through the commit channel.
func (d Deliver) Commit(m Message) {
	res := Response{
		Success:    false,
		Identifier: m.Identifier,
		Data:       nil,
		Extra:      nil,
		Failure:    nil,
	}
	d.log.Debugf("commit request %#v", m)
	entry := &Entry{
		Operation:      m.Content.Operation,
		Identifier:     m.Identifier,
		Key:            m.Content.Key,
		FinalTimestamp: m.Timestamp,
		Data:           m.Content.Content,
		Extensions:     m.Content.Extensions,
	}
	commit, err := d.sm.Commit(entry)
	if err != nil {
		d.log.Errorf("failed to commit %#v. %v", m, err)
		res.Success = false
		res.Failure = err
	} else {
		switch c := commit.(type) {
		case *Entry:
			res.Success = true
			res.Data = c.Data
			res.Extra = c.Extensions
		default:
			res.Success = false
			res.Failure = fmt.Errorf("commit unknown response. %#v", c)
		}
	}

	select {
	case <-d.ctx.Done():
		return
	case <-time.After(150 * time.Millisecond):
		return
	case d.onCommit <- res:
		return
	}
}

func (d Deliver) Listen() <-chan Response {
	return d.onCommit
}

func canHandleState(p Message) bool {
	return p.State == S0 || p.State == S1 || p.State == S2
}

func sortMessagesSlice(slice []Message) {
	sort.SliceStable(slice, func(i, j int) bool {
		a := slice[i]
		b := slice[j]
		if a.Timestamp == b.Timestamp {
			return strings.Compare(string(a.Identifier), string(b.Identifier)) < 0
		}
		return a.Timestamp < b.Timestamp
	})
}
