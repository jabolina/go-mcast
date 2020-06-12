package mcast

import (
	"sync"
)

// Holds information about a message that
// is currently being processed.
type processing struct {
	// Message UID.
	id UID

	// Message destination.
	destination []ServerAddress

	// The current message state.
	state MessageState

	// The message sequence number.
	sequence uint64
}

func (p *processing) canHandleState() bool {
	return p.state == S0 || p.state == S1 || p.state == S2
}

// Will handle delivery sequencing and commits into the
// unity state machine.
type Deliver struct {
	// State machine to commit changes.
	machine StateMachine

	// Verify if messages conflicts.
	conflict ConflictRelationship

	// Holds information about processing messages.
	messages map[UID]processing

	// Holds information about messages that were delivered.
	delayed map[UID]bool

	// Deliver logger from unity.
	log Logger

	// Lock to execute operations.
	mutex *sync.Mutex
}

func NewDeliver(storage Storage, conflict ConflictRelationship, log Logger) *Deliver {
	return &Deliver{
		messages: make(map[UID]processing),
		delayed: make(map[UID]bool),
		conflict: conflict,
		mutex: &sync.Mutex{},
		log: log,
		machine: NewStateMachine(storage),
	}
}

// Add or update the state for a received rpc call.
func (d *Deliver) Add(rpc RPC) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	switch cmd := rpc.Command.(type) {
	case *GMCastRequest:
		var destination []ServerAddress
		for _, server := range cmd.Destination {
			destination = append(destination, server.Address)
		}
		p := processing{
			id:       cmd.UID,
			state:    S0,
			sequence: 0,
			destination: destination,
		}
		_, delivered := d.delayed[cmd.UID]
		if !delivered {
			d.log.Debugf("added to deliver history %#v", cmd)
			d.messages[cmd.UID] = p
		}
	case *ComputeRequest:
		var destination []ServerAddress
		for _, server := range cmd.Destination {
			destination = append(destination, server.Address)
		}
		p := processing{
			id:       cmd.UID,
			state:    cmd.State,
			sequence: cmd.Timestamp,
			destination: destination,
		}
		_, delivered := d.delayed[cmd.UID]
		if !delivered {
			d.log.Debugf("added to deliver history %#v", cmd)
			d.messages[cmd.UID] = p
		}
	case *GatherRequest:
		var destination []ServerAddress
		for _, server := range cmd.Destination {
			destination = append(destination, server.Address)
		}
		p := processing{
			id:       cmd.UID,
			state:    cmd.State,
			sequence: cmd.Timestamp,
			destination: destination,
		}
		_, delivered := d.delayed[cmd.UID]
		if !delivered {
			d.log.Debugf("added to deliver history %#v", cmd)
			d.messages[cmd.UID] = p
		}
	}
}

func (d *Deliver) Delete(uid UID) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.delayed[uid] = true
	delete(d.messages, uid)
}

// Deliver the given response for the given rpc request.
func (d *Deliver) Deliver(data, extension []byte, uid UID, res *GMCastResponse) {
	d.mutex.Lock()
	values := d.messages
	d.mutex.Unlock()

	p, ok := values[uid]
	if ok {
		p.sequence = res.SequenceNumber
		delete(values, uid)
		d.process(data, extension, p, res, values)
	}

}

// Verify if request can be delivered.
// If can be delivered now, the response will be committed
// into the state machine.
func (d *Deliver) process(data, extension []byte, p processing, res *GMCastResponse, values map[UID]processing) {
	entry := &Entry{
		FinalTimestamp: p.sequence,
		Data:           data,
		Extensions:     extension,
	}

	if len(values) == 0 {
		d.Commit(p.id, entry, res)
	}

	for _, m := range values {
		if m.id != p.id && m.canHandleState() && !d.conflict.Conflicts(p.destination) && p.sequence < m.sequence {
			d.Commit(p.id, entry, res)
		}
	}
}

func (d *Deliver) Commit(uid UID, entry *Entry, res *GMCastResponse) {
	defer d.Delete(uid)

	d.log.Debugf("commit request %s into state machine", uid)

	commit, err := d.machine.Commit(entry)
	failed := Message{
		MessageState: S0,
		Timestamp:    0,
		Data:         nil,
		Extensions:   nil,
	}
	if err != nil {
		d.log.Debugf("failed commit %v", err)
		res.Success = false
		res.Body = failed
		return
	}

	switch m := commit.(type) {
	case Message:
		res.Success = true
		res.Body = m
	default:
		d.log.Debugf("commit unknown response. %#v", m)
		res.Success = false
		res.Body = failed
	}
}
