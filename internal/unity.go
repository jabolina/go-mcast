package internal

import (
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
	Write(request Request) (UID, error)

	// Query a value from the unity.
	Read(request Request) (Response, error)

	// Shutdown the unity.
	// This is NOT a graceful shutdown, everything that
	// is going on will stop.
	Shutdown()
}

// Concrete implementation of the Unity interface.
type PeerUnity struct {
	// Hold all peers.
	peers []PartitionPeer

	configuration *Configuration

	last int

	invoker *Invoker
}

func NewUnity(configuration *Configuration) (Unity, error) {
	invk := &Invoker{group: &sync.WaitGroup{}}
	var peers []PartitionPeer
	for i := 0; i < configuration.Replication; i++ {
		pc := &PeerConfiguration{
			Name:      GenerateUID(),
			Partition: configuration.Name,
			Version:   configuration.Version,
			Invoker:   invk,
			Conflict:  configuration.Conflict,
			Storage:   configuration.Storage,
		}
		peer, err := NewPeer(pc, configuration.Logger)
		if err != nil {
			return nil, err
		}

		peers = append(peers, peer)
	}
	pu := &PeerUnity{
		configuration: configuration,
		peers:         peers,
		last:          0,
		invoker:       invk,
	}
	return pu, nil
}

// Implements the Unity interface.
func (p *PeerUnity) Write(request Request) (UID, error) {
	id := UID(GenerateUID())
	message := Message{
		Header: ProtocolHeader{
			ProtocolVersion: p.configuration.Version,
			Type:            Initial,
		},
		Identifier: id,
		Content: DataHolder{
			Operation:  Command,
			Key:        request.Key,
			Content:    request.Value,
			Extensions: request.Extra,
		},
		State:       S0,
		Timestamp:   0,
		Destination: request.Destination,
	}
	peer := p.resolveNextPeer()
	return id, peer.Transport().Broadcast(message)
}

// Implements the Unity interface.
func (p *PeerUnity) Read(request Request) (Response, error) {
	peer := p.resolveNextPeer()
	return peer.FastRead(request)
}

// Implements the Unity interface.
func (p *PeerUnity) Shutdown() {
	for _, peer := range p.peers {
		peer.Stop()
	}
	p.invoker.group.Wait()
}

// Returns the next peer to be used. This will
// work as a round robin chain.
func (p PeerUnity) resolveNextPeer() PartitionPeer {
	defer func() {
		p.last += 1
	}()
	return p.peers[p.last%len(p.peers)]
}
