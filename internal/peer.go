package internal

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// When sending a message the peer must choose
// which kind of message will be emitted.
type emission = uint

const (
	// When emitting a message only internally inside
	// the partition. This is used when the message is
	// on State S0 or S2.
	inner emission = iota

	// Used when exchanging the message Timestamp between
	// the other partitions that participate on the protocol.
	outer
)

// An observer that waits until the issued request
// is committed by one of the peers.
// When the response is committed it will be sent
// back through the observer channel.
type observer struct {
	// Request UID.
	uid UID

	// Channel to notify the response back.
	notify chan Response
}

// Interface that a single peer must implement.
type PartitionPeer interface {
	// Issues a request to the Generic Multicast protocol.
	//
	// This method does not work in the request-response model
	// so after the message is committed onto the unity
	// a response will be sent back through the channel.
	Command(message Message) <-chan Response

	// A fast read directly into the storage.
	// Since all peers will be consistent, the read
	// operations can be done directly into the storage.
	//
	// See that if a write was issued, is not guaranteed
	// that the read will be executed after the write.
	FastRead(request Request) (Response, error)

	// Stop the peer.
	Stop()
}

// This structure defines a single peer for the protocol.
// A group of peers will form a single partition, so,
// a single peer is not fault tolerant, but a partition
// will be.
type Peer struct {
	// Mutex for synchronizing operations.
	mutex *sync.Mutex

	// Holds the observers that are waiting for a response
	// from the issued request.
	observers map[UID]observer

	// Configuration for the peer.
	configuration *PeerConfiguration

	// Transport used for communication between peers
	// and between partitions.
	transport Transport

	// The peer clock for defining a message timestamp.
	clock LogicalClock

	// The peer received queue, to order the requests.
	rqueue Queue

	// Previous set for the peer.
	previousSet PreviousSet

	// Process responsible to deliver messages on the
	// right order.
	deliver Deliverable

	// Holds the peer storage, this will be used
	// for reads only, all writes will come from the
	// state machine when a commit is applied.
	storage Storage

	// Conflict relationship for ordering the messages.
	conflict ConflictRelationship

	// Peer logger.
	log Logger

	// When external requests exchange timestamp,
	// this will hold the received values.
	received *Memo

	// When a message state is updated locally
	// and need to trigger the process again.
	updated chan Message

	// The peer cancellable context.
	context context.Context

	// A cancel function to finish the peer processing.
	finish context.CancelFunc
}

// Creates a new peer for the given configuration and
// start polling for new messages.
func NewPeer(configuration *PeerConfiguration, log Logger) (PartitionPeer, error) {
	t, err := NewTransport(configuration, log)
	if err != nil {
		return nil, err
	}

	ctx, done := context.WithCancel(context.Background())
	deliver, err := NewDeliver(ctx, log, configuration.Conflict, configuration.Storage)
	if err != nil {
		done()
		return nil, err
	}

	p := &Peer{
		mutex:         &sync.Mutex{},
		observers:     make(map[UID]observer),
		configuration: configuration,
		transport:     t,
		clock:         &ProcessClock{
			mutex: &sync.Mutex{},
		},
		previousSet:   NewPreviousSet(),
		deliver:       deliver,
		storage:       configuration.Storage,
		conflict:      configuration.Conflict,
		log:           log,
		received:      NewMemo(),
		updated:       make(chan Message),
		context:       ctx,
		finish:        done,
	}
	applyDeliver := func(i interface{}) {
		p.doDeliver(i.(Message))
	}
	p.rqueue = NewQueue(ctx, configuration.Invoker, configuration.Conflict, applyDeliver)
	p.configuration.Invoker.invoke(p.poll)
	return p, nil
}

// Implements the PartitionPeer interface.
func (p *Peer) Command(message Message) <-chan Response {
	res := make(chan Response)
	apply := func() {
		err := p.transport.Broadcast(message)
		if err != nil {
			finalResponse := Response{
				Success:    false,
				Identifier: message.Identifier,
				Data:       message.Content.Content,
				Extra:      message.Content.Extensions,
				Failure:    err,
			}

			select {
			case res <- finalResponse:
			case <-time.After(100 * time.Millisecond):
			}
			return
		}

		p.mutex.Lock()
		defer p.mutex.Unlock()
		obs := observer{
			uid:    message.Identifier,
			notify: res,
		}
		p.observers[message.Identifier] = obs
	}
	p.configuration.Invoker.invoke(apply)
	return res
}

// Implements the PartitionPeer interface.
func (p *Peer) FastRead(request Request) (Response, error) {
	res := Response{
		Success:    false,
		Identifier: "",
		Data:       nil,
		Extra:      nil,
		Failure:    nil,
	}
	data, err := p.storage.Get(request.Key)
	if err != nil {
		res.Failure = err
		return res, err
	}
	var entry Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		res.Failure = err
		return res, nil
	}

	res.Success = true
	res.Identifier = entry.Identifier
	res.Data = entry.Data
	res.Extra = entry.Extensions
	return res, nil
}

// Implements the PartitionPeer interface.
func (p *Peer) Stop() {
	defer func() {
		close(p.updated)
	}()
	p.finish()
	p.transport.Close()
}

// This method will keep polling as long as the peer
// is active.
// Listening for messages received from the transport
// and processing following the protocol definition.
// If the context is cancelled, this method will stop.
func (p *Peer) poll() {
	defer p.log.Debugf("closing the peer %s", p.configuration.Name)
	for {
		select {
		case <-p.context.Done():
			return
		case m, ok := <-p.updated:
			if !ok {
				return
			}
			p.configuration.Invoker.invoke(func() {
				p.send(m, Initial, inner)
			})
		case m, ok := <-p.transport.Listen():
			if !ok {
				return
			}
			p.configuration.Invoker.invoke(func() {
				p.process(m)
			})
		}
	}
}

// Process the received message from the transport.
// First verify if the current configured peer can handle
// this request version.
//
// If the process can be handled, the message is then processed,
// the message must be of type initial or of type external.
//
// After processing the message, updates the value on the
// received queue and then trigger the deliver method to
// start commit on the state machine.
func (p Peer) process(message Message) {
	header := message.Extract()
	if header.ProtocolVersion != p.configuration.Version {
		p.log.Warnf("peer not processing message %#v on version %d", message, header.ProtocolVersion)
		return
	}

	if !p.rqueue.IsEligible(message) {
		return
	}
	enqueue := true
	defer func() {
		if enqueue {
			p.finishMessageProcessing(&message)
		}
	}()

	switch header.Type {
	case Initial:
		p.log.Debugf("processing internal request %#v", message)
		p.processInitialMessage(&message)
	case External:
		p.log.Debugf("processing external request %#v", message)
		enqueue = p.exchangeTimestamp(&message)
	default:
		p.log.Warnf("unknown message type %d", header.Type)
		enqueue = false
	}
}

// After the process GB-Deliver m, if m.State is equals to S0, firstly the
// algorithm check if m conflict with any other message on previousSet,
// if so, the process p increment its local clock and empty the previousSet.
// At last, the message m receives its group timestamp and is added to
// previousSet maintaining the information about conflict relations to
// future messages.
//
// On the second part of this procedure, the process p checks if m.Destination
// has only one destination, if so, message m can jump to state S3, since its
// not necessary to exchange timestamp information between others destination
// groups and a second consensus can be avoided due to group timestamp is now a
// final timestamp.
//
// Otherwise, for messages on state S0, we set the group timestamp to the value
// of the process clock, update m.State to S1 and send m to all others groups in
// m.Destination. On the other hand, to messages on state S2, the message has the
// final timestamp, thus m.State can be updated to the final state S3 and, if
// m.Timestamp is greater than local clock value, the clock is updated to hold
// the received timestamp and the previousSet can be cleaned.
func (p *Peer) processInitialMessage(message *Message) {
	if message.State == S0 {
		if p.conflict.Conflict(*message, p.previousSet.Snapshot()) {
			p.clock.Tick()
			p.previousSet.Clear()
		}
		message.Timestamp = p.clock.Tock()
		p.previousSet.Append(*message)
	}

	if len(message.Destination) > 1 {
		if message.State == S0 {
			message.State = S1
			message.Timestamp = p.clock.Tock()
			p.received.Insert(message.Identifier, p.configuration.Partition, message.Timestamp)
			p.send(*message, External, outer)
		} else if message.State == S2 {
			message.State = S3
			if message.Timestamp > p.clock.Tock() {
				p.clock.Leap(message.Timestamp)
				p.previousSet.Clear()
			}
		}
	} else {
		message.Timestamp = p.clock.Tock()
		message.State = S3
	}
}

// When a message m has more than one destination group, the destination groups
// have to exchange its timestamps to decide the final timestamp to m.
// Thus, after receiving all other timestamp values, a temporary variable tsm is
// agree upon the maximum timestamp value received.
//
// Once the algorithm have select the tsm value, the process checks if m.Timestamp
// is greater or equal to tsm, in positive case, a second consensus instance can be
// avoided and, the state of m can jump directly to state S3 since the group local
// clock is already bigger than tsm.
func (p *Peer) exchangeTimestamp(message *Message) bool {
	p.received.Insert(message.Identifier, message.From, message.Timestamp)
	values := p.received.Read(message.Identifier)
	if len(values) < len(message.Destination) {
		return false
	}

	tsm := MaxValue(values)
	if message.Timestamp >= tsm {
		message.State = S3
	} else {
		message.Timestamp = tsm
		message.State = S2
	}
	return true
}

// Used to send a request using the transport API.
// Used for request across partitions, when exchanging the
// message timestamp or when broadcasting the message internally
// inside a partition.
func (p Peer) send(message Message, t MessageType, emission emission) {
	message.Header.Type = t
	message.From = p.configuration.Partition
	var destination []Partition
	if emission == inner {
		destination = append(destination, p.configuration.Partition)
	} else {
		for _, partition := range message.Destination {
			if partition != p.configuration.Partition {
				destination = append(destination, partition)
			}
		}
	}

	for _, partition := range destination {
		for err := p.transport.Unicast(message, partition); err != nil; {
			p.log.Errorf("error unicast %s to partition %s. %v", message.Identifier, partition, err)
		}
	}
}

// After the message is processed by the protocol, the value
// will be updated on the rqueue, and if the message is on the
// state S0 or S2 it needs to be broadcast internally to the
// partition.
func (p *Peer) finishMessageProcessing(message *Message) {
	defer func() {
		recover()
	}()

	p.rqueue.Enqueue(*message)
	uid := message.Identifier
	p.configuration.Invoker.invoke(func() {
		p.reprocessMessage(uid)
	})
}

// Verify if the given message needs to be resend
// to the processes inside a partition.
func (p Peer) reprocessMessage(uid UID) {
	value := p.rqueue.GetIfExists(string(uid))
	if value == nil {
		return
	}
	message := value.(Message)
	if message.State == S0 || message.State == S2 {
		select {
		case <-p.context.Done():
			return
		case <-time.After(100 * time.Millisecond):
			p.reprocessMessage(uid)
			return
		case p.updated <- message:
			return
		}
	}

	if message.State == S3 {
		p.rqueue.GenericDeliver(message)
	}
}

// The doDeliver method to commit the element on the head
// of the rqueue. Since the rqueue will be already sorted,
// both by the timestamp and by the message UID, we have
// the guarantee that when a message on the head is on the
// state S3 it will be the right message to be delivered.
//
// Since a message on state S3 already has its final timestamp,
// and since the message is on the head of the rqueue it also
// contains the lowest timestamp, so the message is ready to
// be delivered, which means, it will be committed on the
// local peer state machine.
func (p *Peer) doDeliver(m Message) {
	p.received.Remove(m.Identifier)
	res := p.deliver.Commit(m)
	p.configuration.Invoker.invoke(func() {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		obs, ok := p.observers[m.Identifier]
		if ok {
			select {
			case <-time.After(150 * time.Millisecond):
				break
			case obs.notify <- res:
				break
			}
			close(obs.notify)
			delete(p.observers, obs.uid)
		}
	})
}
