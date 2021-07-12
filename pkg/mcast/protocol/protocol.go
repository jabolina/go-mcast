package protocol

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/hpq"
	"github.com/jabolina/go-mcast/pkg/mcast/output"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// The Protocol structure is the actual Generic Multicast implementation.
// To create a working protocol, some extra structures are also needed and are
// used as helpers.
type Protocol struct {
	// The Mem structure on the specification. Responsible for holding all the
	// information about messages currently in process and already delivered
	// messages. Also notifies when a message is ready to be delivered.
	Mem hpq.Memory

	// The channel memoryNotification is used by the Mem structure to notify
	// the Protocol about messages that are ready to be delivered.
	memoryNotification chan hpq.ElementNotification

	// The previousSet structure also defined in the specification. This structure
	// holds messages that are currently processed and also holds the conflict
	// relationship and the previous set know when exists a conflict between messages.
	previousSet PreviousSet

	// The clock is the current process clock. This is not shared with any other process
	// and even within a group of processes, each process will hold its own clock.
	clock LogicalClock

	// The ballotBox is responsible to gather information about the votes for a final
	// sequence number for a message.
	ballotBox *BallotBox

	// The deliver structure is the output of the protocol, used when a message is ready
	// to be delivered, this will start the process after a message is finished processing.
	// Meaning that will commit to the state machine and the storage.
	deliver output.Deliverable

	// The commit channel is used to notify a listening client about committed entries.
	commit chan<- types.Response

	// The invoker is a helper structure to spawn new managed goroutines without leaking.
	invoker helper.Invoker
}

// NewProcessProtocol will create a new Protocol structure and start all
// needed sub-structures and goroutines.
func NewProcessProtocol(
	commit chan<- types.Response,
	parent context.Context,
	set PreviousSet,
	deliverable output.Deliverable,
	invoker helper.Invoker) *Protocol {

	p := &Protocol{
		memoryNotification: make(chan hpq.ElementNotification),
		previousSet:        set,
		clock:              NewClock(),
		ballotBox:          NewBallotBox(),
		commit:             commit,
		deliver:            deliverable,
		invoker:            invoker,
	}
	p.Mem = hpq.NewReceivedQueue(parent, p.memoryNotification, func(message types.Message) bool {
		return p.previousSet.ExistsConflict(message)
	})
	invoker.Spawn(p.handleMemoryNotifications)
	return p
}

// The ReceiveMessage is the only entry point when using the protocol. Through
// this method a message is dispatched so the protocol can handle it accordingly.
// After the message is processed, is up to the client to call the next step execution,
// the protocol will return what must be executed as next step.
func (p *Protocol) ReceiveMessage(message *types.Message) Step {
	response := func() Step {
		switch message.Extract().Type {
		case types.ABCast:
			return p.computeGroupSeqNumber(message)
		case types.Network:
			return p.gatherGroupsTimestamps(message)
		default:
			return NoOp
		}
	}()
	return p.collectAfterProcessing(message, response)
}

// The collectAfterProcessing is called after a message finished processing,
// called after each step with the returned value for the next step.
func (p *Protocol) collectAfterProcessing(message *types.Message, step Step) Step {
	if step == NoOp {
		return step
	}

	p.Mem.Append(*message)
	if step == Ended {
		p.Mem.GenericDeliver(*message)
	}
	return step
}

// After the process AB-Deliver m, if m.State is equals to S0, firstly the
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
// Otherwise, for messages on state S0, we priorityQueue the group timestamp to the value
// of the process clock, update m.State to S1 and send m to all others groups in
// m.Destination. On the other hand, to messages on state S2, the message has the
// final timestamp, thus m.State can be updated to the final state S3 and, if
// m.Timestamp is greater than local clock value, the clock is updated to hold
// the received timestamp and the previousSet can be cleaned.
func (p *Protocol) computeGroupSeqNumber(message *types.Message) Step {
	if !p.Mem.Acceptable(*message) {
		return NoOp
	}

	if message.State == types.S0 {
		if p.previousSet.ExistsConflict(*message) {
			p.clock.Tick()
			p.previousSet.Clear()
		}
		p.previousSet.Append(*message)
	}

	if len(message.Destination) > 1 {
		if message.State == types.S0 {
			message.Timestamp = p.clock.Tock()
			message.State = types.S1
			return ExchangeAll
		}

		if message.State == types.S2 {
			if message.Timestamp > p.clock.Tock() {
				p.clock.Leap(message.Timestamp)
				p.previousSet.Clear()
			}
			message.State = types.S3
			return Ended
		}

		return NoOp
	}

	message.Timestamp = p.clock.Tock()
	message.State = types.S3
	return Ended
}

// When a message m has more than one destination group, the destination groups
// have to exchange its timestamps to decide the final timestamp to m.
// Thus, after receiving all other timestamp votes, a temporary variable tsm is
// agree upon the maximum timestamp value received.
//
// Once the algorithm have select the tsm value, the process checks if m.Timestamp
// is greater or equal to tsm, in positive case, a second consensus instance can be
// avoided and, the state of m can jump directly to state S3 since the group local
// clock is already bigger than tsm.
func (p *Protocol) gatherGroupsTimestamps(message *types.Message) Step {
	if !p.shouldProceedGatherGroupsTimestamps(message) {
		return NoOp
	}

	votes := p.ballotBox.Read(message.Identifier)
	tsMax := helper.MaxValue(votes)

	if message.Timestamp >= tsMax {
		message.State = types.S3
		return Ended
	} else {
		message.Timestamp = tsMax
		message.State = types.S2
		return ExchangeInternal
	}
}

// The doDeliver is called to commit the given element. Since the Mem will sort messages,
// both by the timestamp and by the message UID, we have the guarantee that when a message
// on the head is on the state S3 it will be the right message to be delivered. So the
// structure will automatically notify about a message to be delivered. The Mem structure will
// also notify when a message can be generic delivered.
//
// Since a message on state S3 already has its final timestamp, and since the message is on
// the head of the queue it also contains the lowest timestamp, so the message is ready to be
// delivered, which means, it will be committed on the local peer state machine.
func (p *Protocol) doDeliver(message types.Message, generic bool) {
	select {
	case p.commit <- p.deliver.Commit(message, generic):
		break
	default:
		break
	}

	p.invoker.Spawn(func() {
		p.ballotBox.Remove(message.Identifier)
		p.Mem.Remove(message)
	})
}

func (p *Protocol) Close() error {
	return p.Mem.Close()
}

// Method responsible for received notifications from the Mem structure.
// A notification will be published when a message is ready to be delivered.
func (p *Protocol) handleMemoryNotifications() {
	for notification := range p.memoryNotification {
		m, isGenericDeliver := notification.Value.(types.Message), notification.OnApply
		p.doDeliver(m, isGenericDeliver)
	}
}

// The shouldProceedGatherGroupsTimestamps verify if the method can be executed now.
// From the specification there is a guard that must be fulfilled to execute this step,
// this guard in english states that:
//
// Exists a message `m` in state `S1` in the Mem structure, and exists a process that
// belongs to a destination partition that sent a message to exchange the timestamp.
//
// This verify that, for a message with destinations `A`, `B` and `C`, at least a single
// process from each partition `A`, `B` and `C` sent a message to exchange the timestamp.
// The vote from the process will only be persisted if exists a message in the Mem structure
// in state `S1`.
func (p *Protocol) shouldProceedGatherGroupsTimestamps(message *types.Message) bool {
	/*alreadyExistsInStateS1 := func(m types.Message) bool {
		return m.State == types.S1 && m.Identifier == message.Identifier
	}

	if !p.Mem.Exists(alreadyExistsInStateS1) {
		return false
	}*/

	p.ballotBox.Insert(message.Identifier, message.From, message.Timestamp)
	return p.ballotBox.ElectionSize(message.Identifier) >= len(message.Destination)
}
