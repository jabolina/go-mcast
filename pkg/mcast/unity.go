package mcast

import (
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
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
	Write(request types.Request) <-chan types.Response

	// Query a value from the unity.
	Read(request types.Request) (types.Response, error)

	// Shutdown the unity.
	// This is NOT a graceful shutdown, everything that
	// is going on will stop.
	Shutdown()
}

// Concrete implementation of the Unity interface.
type PeerUnity struct {
	// Hold all peers.
	Peers []core.PartitionPeer

	// Hold the configuration for the whole unity.
	Configuration *types.Configuration

	// Used to iterate amongst all peers in a
	// round robin way.
	Last int

	// Used to spawn and control go routines.
	Invoker core.Invoker
}

func NewUnity(configuration *types.Configuration) (Unity, error) {
	invk := core.InvokerInstance()
	var peers []core.PartitionPeer
	for i := 0; i < configuration.Replication; i++ {
		pc := &types.PeerConfiguration{
			Name:      fmt.Sprintf("%s-%d", configuration.Name, i),
			Partition: configuration.Name,
			Version:   configuration.Version,
			Conflict:  configuration.Conflict,
			Storage:   configuration.Storage,
		}
		peer, err := core.NewPeer(pc, configuration.Logger)
		if err != nil {
			return nil, err
		}

		peers = append(peers, peer)
	}
	pu := &PeerUnity{
		Configuration: configuration,
		Peers:         peers,
		Last:          0,
		Invoker:       invk,
	}
	return pu, nil
}

// Implements the Unity interface.
func (p *PeerUnity) Write(request types.Request) <-chan types.Response {
	id := types.UID(helper.GenerateUID())
	message := types.Message{
		Header: types.ProtocolHeader{
			ProtocolVersion: p.Configuration.Version,
			Type:            types.Initial,
		},
		Identifier: id,
		Content: types.DataHolder{
			Operation:  types.Command,
			Key:        request.Key,
			Content:    request.Value,
			Extensions: request.Extra,
		},
		State:       types.S0,
		Timestamp:   0,
		Destination: request.Destination,
		From:        p.Configuration.Name,
	}
	peer := p.resolveNextPeer()
	p.Configuration.Logger.Infof("sending request %#v", request)
	return peer.Command(message)
}

// Implements the Unity interface.
func (p *PeerUnity) Read(request types.Request) (types.Response, error) {
	peer := p.resolveNextPeer()
	return peer.FastRead(request)
}

// Implements the Unity interface.
func (p *PeerUnity) Shutdown() {
	for _, peer := range p.Peers {
		peer.Stop()
	}
	p.Invoker.Stop()
}

// Returns the next peer to be used. This will
// work as a round robin chain.
func (p PeerUnity) resolveNextPeer() core.PartitionPeer {
	defer func() {
		p.Last += 1
	}()
	return p.Peers[p.Last%len(p.Peers)]
}
