package network

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/protocol"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
)

// Network interface that a single peer must implement.
type Network interface {
	io.Closer

	// Command Issues a request to the Generic Multicast protocol.
	//
	// This method does not work in the request-response model
	// so after the message is committed onto the unity
	// a response will be sent back through the channel.
	Command(message types.Message) error

	// FastRead reads directly into the storage.
	// Since all peers will be consistent, the read
	// operations can be done directly into the storage.
	//
	// See that if a write was issued, is not guaranteed
	// that the read will be executed after the write.
	FastRead() types.Response
}

// Manager is a structure that defines a single peer for the protocol.
// A group of peers will form a single partition, so,
// a single peer is not fault tolerant, but a partition
// will be.
type Manager struct {
	// Used to spawn and control all go routines.
	invoker helper.Invoker

	// Configuration for the peer.
	configuration *types.PeerConfiguration

	// Transport used for communication between peers
	// and between partitions.
	reliableTransport Transport

	// Transport used for unreliable communication.
	unreliableTransport Transport

	protocol protocol.Protocol

	// NetworkManager logger.
	logger types.Logger

	// The peer cancellable context.
	context context.Context

	// A cancel function to finish the peer processing.
	finish context.CancelFunc
}

// NewNetworkManager creates a new peer for the given configuration and
// start polling for new messages.
func NewNetworkManager(
	configuration *types.PeerConfiguration,
	oracle types.Oracle,
	logger types.Logger,
	invoker helper.Invoker) (Network, error) {
	reliableTransport, err := NewReliableTransport(configuration, logger)
	if err != nil {
		return nil, err
	}

	unreliableTransport, err := NewUnreliableTransport(configuration, logger, oracle)
	if err != nil {
		return nil, err
	}

	pl, err := protocol.NewProtocol(*configuration, invoker)
	if err != nil {
		return nil, err
	}

	p := &Manager{
		invoker:             invoker,
		configuration:       configuration,
		reliableTransport:   reliableTransport,
		unreliableTransport: unreliableTransport,
		logger:              logger,
		protocol:            pl,
		context:             configuration.Ctx,
		finish:              configuration.Cancel,
	}
	p.invoker.Spawn(p.poll)
	return p, nil
}

// Command Implements the Network interface.
func (p *Manager) Command(message types.Message) error {
	return p.reliableTransport.Broadcast(message)
}

// FastRead Implements the Network interface.
func (p *Manager) FastRead() types.Response {
	return p.protocol.Read()
}

// Close Implements the Network interface.
func (p *Manager) Close() error {
	p.finish()
	if err := p.protocol.Close(); err != nil {
		return err
	}
	if err := p.reliableTransport.Close(); err != nil {
		return err
	}
	return p.unreliableTransport.Close()
}

// This method will keep polling as long as the peer
// is active.
// Listening for messages received from the reliableTransport
// and processing following the protocol definition.
// If the context is cancelled, this method will stop.
func (p *Manager) poll() {
	defer p.logger.Debugf("closing the peer %s", p.configuration.Name)
	for {
		select {
		case <-p.context.Done():
			return
		case m, ok := <-p.reliableTransport.Listen():
			if !ok {
				return
			}
			p.process(m)
		case m, ok := <-p.unreliableTransport.Listen():
			if !ok {
				return
			}
			p.invoker.Spawn(func() {
				p.process(m)
			})
		}
	}
}

// Start processing the received message using the protocol. First verify if the current
// configured peer can handle this request version.
// If the process can be handled, the message is then processed by the protocol that will
// return what must be done next with the message.
// The processing for the next step can be done detached.
func (p *Manager) process(message types.Message) {
	header := message.Extract()
	if header.ProtocolVersion != p.configuration.Version {
		p.logger.Warnf("peer not processing message %#v on version %d", message, header.ProtocolVersion)
		return
	}

	nextStep := p.protocol.Process(&message)
	p.invoker.Spawn(func() {
		p.handleProtocolNextStep(message, nextStep)
	})
}

// Responsible do choose adequately the next step after processing a message in the protocol.
// As a response after processing, the needed step is returned, which is processed here,
// if the step is `protocol.ExchangeInternal`, this means that internally a single partition the
// processes lost synchronization so the step requires that an atomic broadcast must
// be executed within a single partition to synchronize the processes.
//
// If the next step is `protocol.ExchangeAll`, means that a message must be sent to all
// processes that participate in the protocol, so the unreliable transport is used to
// send the message to all processes.
//
// Any other step does not need to be handled.
func (p *Manager) handleProtocolNextStep(message types.Message, step protocol.Step) {
	switch step {
	case protocol.ExchangeInternal:
		p.invoker.Spawn(func() {
			p.send(message, types.ABCast)
		})
	case protocol.ExchangeAll:
		p.invoker.Spawn(func() {
			p.send(message, types.Network)
		})
	default:
		return
	}
}

// Used to send a request using the reliableTransport API. Used for request across partitions,
// when exchanging the message timestamp or when broadcasting the message internally
// inside a partition.
func (p *Manager) send(message types.Message, t types.MessageType) {
	message.Header.Type = t
	message.From = p.configuration.Partition
	var destination []types.Partition
	if t == types.ABCast {
		destination = append(destination, p.configuration.Partition)
	} else {
		destination = append(destination, message.Destination...)
	}

	for _, partition := range destination {
		if err := p.dispatch(message, partition, t); err != nil {
			p.logger.Errorf("error dispatch. %v. dest: %s -> %#v", err, partition, message)
		}
	}
}

func (p *Manager) dispatch(message types.Message, partition types.Partition, t types.MessageType) error {
	if t == types.Network {
		return p.unreliableTransport.Unicast(message, partition)
	}
	return p.reliableTransport.Unicast(message, partition)
}
