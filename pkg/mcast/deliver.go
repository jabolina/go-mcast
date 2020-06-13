package mcast

import (
	"fmt"
	"math"
	"sort"
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

	// Channel where the response will be sent back
	// when is ready to be delivered.
	notify chan DeliverResponse

	// Message that is being carried on the request.
	// This will be filled when a request is added and
	// only queried when committing into the state machine.
	message Message

	// This will be filled when the request
	// is able to be delivered.
	res *GMCastResponse
}

// A delivery response to be sent back through the
// channel after a message was processed.
type DeliverResponse struct {
	// Holds the final response if success or nil otherwise.
	process *GMCastResponse

	// Holds and error if the deliver failed or nil otherwise.
	err error
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

	// Deliver logger from unity.
	log Logger

	// Lock to execute operations.
	mutex *sync.Mutex

	// Handle spawned goroutines for processing.
	spawn *sync.WaitGroup
}

func NewDeliver(storage Storage, conflict ConflictRelationship, log Logger) *Deliver {
	return &Deliver{
		messages: make(map[UID]processing),
		conflict: conflict,
		mutex:    &sync.Mutex{},
		spawn:    &sync.WaitGroup{},
		log:      log,
		machine:  NewStateMachine(storage),
	}
}

// When the protocol receives a new message to
// be GM-cast, first it will be added into the
// delivery records and a channel will be sent back
// so the caller can be notified when the response is
// ready to be sent back.
// This must be called only on the initial processing
// of a GMCastRequest and must called only once, so there
// is no danger of overriding a channel nor a response.
func (d *Deliver) Add(req *GMCastRequest) <-chan DeliverResponse {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.insert(req)
}

// This will add a new request into the memory and *must* be called
// when holding the lock.
func (d *Deliver) insert(req *GMCastRequest) <-chan DeliverResponse {
	var destination []ServerAddress
	for _, server := range req.Destination {
		destination = append(destination, server.Address)
	}

	if stored, existent := d.messages[req.UID]; existent {
		return stored.notify
	}

	channel := make(chan DeliverResponse)
	p := processing{
		id:          req.UID,
		state:       S0,
		sequence:    0,
		destination: destination,
		notify:      channel,
		message:     req.Body,
	}
	d.log.Debugf("added to deliver history %#v", req)
	d.messages[req.UID] = p
	return channel
}

// For each step, when an RPC is received the message
// status must be updated, so everything is kept up-to-date.
// If the there is no message with the RPC UID on memory,
// nothing will be done.
func (d *Deliver) Update(rpc RPC) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	switch cmd := rpc.Command.(type) {
	case *ComputeRequest:
		p, existent := d.messages[cmd.UID]
		if !existent {
			return
		}
		p.state = cmd.State
		d.messages[p.id] = p
	case *GatherRequest:
		p, existent := d.messages[cmd.UID]
		if !existent {
			return
		}
		p.state = cmd.State
		d.messages[p.id] = p
	}
}

func (d *Deliver) Delete(uid UID) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	p, exists := d.messages[uid]
	if exists {
		close(p.notify)
	}
	delete(d.messages, uid)
}

// Deliver the given response for the given rpc request.
// If the request is not present in the messages history
// creates a new channel and answer back the caller and
// spawn the processing goroutine to start delivering the requests.
func (d *Deliver) Deliver(req *GMCastRequest, res *GMCastResponse) <-chan DeliverResponse {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	value, existent := d.messages[req.UID]
	if !existent {
		d.insert(req)
		value = d.messages[req.UID]
	}

	value.sequence = res.SequenceNumber
	value.state = S3
	value.res = res
	d.messages[req.UID] = value

	d.spawn.Add(1)
	go d.doDeliver()

	return value.notify
}

// This will start a routine for delivering the requests
// on the right order.
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
func (d *Deliver) doDeliver() {
	defer d.spawn.Done()
	d.mutex.Lock()
	snapshot := d.messages
	d.mutex.Unlock()

	ready := make(map[UID]processing)
	for uid, p := range snapshot {
		if p.state == S3 {
			ready[uid] = p
		}
	}

	// This is the first verification and attempt to deliver messages
	// that are already on state S3 and do not conflicts with other messages.
	// For each message committed, since the channel is closed, the entry
	// will be removed for the snapshot.
	for _, p := range ready {
		if d.deliverNonConflicting(p, snapshot) {
			delete(snapshot, p.id)
		}
	}

	// Then try to deliver the message m with the smaller timestamp
	// amongst all messages, and m on state S3 and not conflicting with
	// any other message.
	smallest := processing{sequence: math.MaxUint64}
	for _, p := range snapshot {
		if p.sequence < smallest.sequence {
			smallest = p
		}
	}

	sample := make(map[UID][]ServerAddress)
	for uid, p := range snapshot {
		sample[uid] = p.destination
	}

	delete(sample, smallest.id)
	if smallest.state == S3 && d.conflict.ConflictsWith(smallest.destination, sample) {
		d.Commit(smallest, smallest.res)
		delete(snapshot, smallest.id)
	}

	// Get all messages on state S3 with same timestamp and deliver them deterministically.
	d.deliveryFinalTimestamps(snapshot)
}

// Deliver a message m that do no conflict with any other message with
// states S0, S1 and S2.
func (d *Deliver) deliverNonConflicting(m processing, values map[UID]processing) bool {
	sample := make(map[UID][]ServerAddress)
	for uid, p := range values {
		if uid != m.id {
			sample[uid] = p.destination
		}
	}

	for uid, p := range values {
		if m.id != uid && p.canHandleState() && !d.conflict.ConflictsWith(m.destination, sample) {
			d.Commit(m, m.res)
			return true
		}
	}
	return false
}

// Delivery the messages that are on state S3 with conflicting timestamps.
// This will group all messages on state S3 that have the same final timestamp,
// the it will sort by the timestamp and then delivery each message.
func (d *Deliver) deliveryFinalTimestamps(values map[UID]processing) {
	ready := make(map[UID]processing)
	for uid, p := range values {
		if p.state == S3 {
			ready[uid] = p
		}
	}

	grouped := make(map[uint64][]processing)
	for _, p := range ready {
		group := grouped[p.sequence]
		group = append(group, p)
		grouped[p.sequence] = group
	}

	keys := make([]uint64, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return i < j
	})

	for _, k := range keys {
		attempt := make(map[UID]processing)
		for _, p := range grouped[k] {
			attempt[p.id] = p
		}
		d.deterministicDeliver(attempt)
	}
}

// Deterministic delivery for messages on state S3 with same sequence number.
func (d *Deliver) deterministicDeliver(values map[UID]processing) {
	var ordered []string
	for uid := range values {
		ordered = append(ordered, string(uid))
	}
	sort.Strings(ordered)

	for _, uid := range ordered {
		p, ok := values[UID(uid)]
		if ok {
			d.Commit(p, p.res)
		}
	}
}

// Commit the res for the give message m that is being processed.
// After the commit a response is sent back through the response
// channel.
// There is a recover ready to capture if a response was written
// in a closed channel.
func (d *Deliver) Commit(m processing, res *GMCastResponse) {
	defer func() {
		if r := recover(); r != nil {
			d.log.Warnf("publish on closed channel for %s", m.id)
		}
		d.Delete(m.id)
	}()

	var commitErr error
	entry := &Entry{
		Operation:      m.message.Data.Operation,
		Key:            m.message.Data.Key,
		FinalTimestamp: m.sequence,
		Data:           m.message.Data.Content,
		Extensions:     m.message.Extensions,
	}

	d.log.Debugf("commit request %s into state machine", m.id)

	commit, err := d.machine.Commit(entry)
	if err != nil {
		d.log.Debugf("failed commit %v", err)
		res.Success = false
		commitErr = err
	} else {
		switch m := commit.(type) {
		case Message:
			res.Success = true
			res.Body = m
		default:
			d.log.Debugf("commit unknown response. %#v", m)
			res.Success = false
			commitErr = fmt.Errorf("commit unknown response. %#v", m)
		}
	}

	m.notify <- DeliverResponse{
		process: res,
		err:     commitErr,
	}
}

// On shutdown wait for all spawned deliver goroutines
// to finish.
func (d *Deliver) Shutdown() {
	d.spawn.Wait()
}
