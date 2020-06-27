package internal

import (
	"fmt"
	"math"
	"sort"
)

// Interface to deliver messages.
type Deliverable interface {
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
func NewDeliver(log Logger, conflict ConflictRelationship, storage Storage) Deliverable {
	return &Deliver{
		onCommit: make(chan Response),
		conflict: conflict,
		sm:       NewStateMachine(storage),
		log:      log,
	}
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
func (d Deliver) Deliver(messages []Message) {
	ready := make(map[UID]Message)
	for _, message := range messages {
		if message.State == S3 {
			ready[message.Identifier] = message
		}
	}

	if len(ready) == 0 {
		return
	}

	// This is the first verification and attempt to deliver messages
	// that are already on state S3 and do not conflicts with other messages.
	// For each message committed, since the channel is closed, the entry
	// will be removed for the snapshot.
	for _, m := range ready {
		if d.deliverNonConflicting(m, ready, messages) {
			delete(ready, m.Identifier)
		}
	}

	// Then try to deliver the message m with the smaller timestamp
	// amongst all messages, and m on state S3 and not conflicting with
	// any other message.
	smallest := Message{Timestamp: math.MaxUint64}
	for _, p := range messages {
		if p.Timestamp < smallest.Timestamp {
			smallest = p
		}
	}

	if smallest.State == S3 && d.conflict.Conflict(smallest, messages) {
		d.Commit(smallest)
		delete(ready, smallest.Identifier)
	}

	// Get all messages on state S3 with same timestamp and deliver them deterministically.
	d.deliveryFinalTimestamps(ready)
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

	d.onCommit <- res
}

func (d Deliver) Listen() <-chan Response {
	return d.onCommit
}

// Deliver a message m that do no conflict with any other message with
// states S0, S1 and S2.
func (d Deliver) deliverNonConflicting(m Message, values map[UID]Message, asSlice []Message) bool {
	for uid, message := range values {
		if m.Identifier != uid && canHandleState(message) && !d.conflict.Conflict(m, asSlice) {
			d.Commit(m)
			return true
		}
	}
	return false
}

// Delivery the messages that are on state S3 with conflicting timestamps.
// This will group all messages on state S3 that have the same final timestamp,
// the it will sort by the timestamp and then delivery each message.
func (d *Deliver) deliveryFinalTimestamps(ready map[UID]Message) {
	grouped := make(map[uint64][]Message)
	for _, p := range ready {
		group := grouped[p.Timestamp]
		group = append(group, p)
		grouped[p.Timestamp] = group
	}

	keys := make([]uint64, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return i < j
	})

	for _, k := range keys {
		attempt := make(map[UID]Message)
		for _, p := range grouped[k] {
			attempt[p.Identifier] = p
		}
		d.deterministicDeliver(attempt)
	}
}

// Deterministic delivery for messages on state S3 with same sequence number.
func (d *Deliver) deterministicDeliver(values map[UID]Message) {
	var ordered []string
	for uid := range values {
		ordered = append(ordered, string(uid))
	}
	sort.Strings(ordered)

	for _, uid := range ordered {
		p, ok := values[UID(uid)]
		if ok {
			d.Commit(p)
		}
	}
}

func canHandleState(p Message) bool {
	return p.State == S0 || p.State == S1 || p.State == S2
}
