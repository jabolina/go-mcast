package mcast

import (
	"fmt"
	"github.com/jabolina/go-mcast/internal"
	"sync"
)

// The unity interface, responsible for interacting
// with all the peers.
// The unity is equivalent as a partition and holds a
// group of peers, each one a different goroutine. When
// interacting with the protocol, every command is
// issued through the unity, since all peers acts
// as a single unity.
type Unity interface {
	// Apply a request to the protocol.
	// This does not work in the request-response model,
	// once a request is sent the method will return right
	// away, but this does not mean that the value is already
	// committed on the state machine.
	// To listen when the request is applied and if it was
	// applied successfully a channel will be returned where
	// a response will be sent back once the request is applied
	// in one of the participants.
	Write(request internal.Request) <-chan internal.Response

	// Query a value from the unity.
	Read(request internal.Request) (internal.Response, error)

	// Shutdown the unity.
	// This is NOT a graceful shutdown, everything that
	// is going on will stop.
	Shutdown()
}

// Concrete implementation of the Unity interface.
type peerUnity struct {
	// Hold all peers.
	peers []internal.PartitionPeer

	// Hold the configuration for the whole unity.
	configuration *internal.Configuration

	// Used to iterate amongst all peers in a
	// round robin way.
	last int

	// Used to spawn and control go routines.
	invoker *internal.Invoker
}

func NewUnity(configuration *internal.Configuration) (Unity, error) {
	invk := &internal.Invoker{Group: &sync.WaitGroup{}}
	var peers []internal.PartitionPeer
	for i := 0; i < configuration.Replication; i++ {
		pc := &internal.PeerConfiguration{
			Name:      fmt.Sprintf("%s-%d", configuration.Name, i),
			Partition: configuration.Name,
			Version:   configuration.Version,
			Invoker:   invk,
			Conflict:  configuration.Conflict,
			Storage:   configuration.Storage,
		}
		peer, err := internal.NewPeer(pc, configuration.Logger)
		if err != nil {
			return nil, err
		}

		peers = append(peers, peer)
	}
	pu := &peerUnity{
		configuration: configuration,
		peers:         peers,
		last:          0,
		invoker:       invk,
	}
	return pu, nil
}

// Implements the Unity interface.
func (p *peerUnity) Write(request internal.Request) <-chan internal.Response {
	id := internal.UID(internal.GenerateUID())
	message := internal.Message{
		Header: internal.ProtocolHeader{
			ProtocolVersion: p.configuration.Version,
			Type:            internal.Initial,
		},
		Identifier: id,
		Content: internal.DataHolder{
			Operation:  internal.Command,
			Key:        request.Key,
			Content:    request.Value,
			Extensions: request.Extra,
		},
		State:       internal.S0,
		Timestamp:   0,
		Destination: request.Destination,
		From:        p.configuration.Name,
	}
	peer := p.resolveNextPeer()
	p.configuration.Logger.Infof("sending request %#v", request)
	return peer.Command(message)
}

// Implements the Unity interface.
func (p *peerUnity) Read(request internal.Request) (internal.Response, error) {
	peer := p.resolveNextPeer()
	return peer.FastRead(request)
}

// Implements the Unity interface.
func (p *peerUnity) Shutdown() {
	for _, peer := range p.peers {
		peer.Stop()
	}
	p.invoker.Group.Wait()
}

// Returns the next peer to be used. This will
// work as a round robin chain.
func (p peerUnity) resolveNextPeer() internal.PartitionPeer {
	defer func() {
		p.last += 1
	}()
	return p.peers[p.last%len(p.peers)]
}
