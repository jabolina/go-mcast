package core

import (
	"context"
	"encoding/json"
	"github.com/digital-comrades/proletariat/pkg/proletariat"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// UnreliableTransport is an instance of the Transport interface that provides
// a simple and unreliable transport for communication.
type UnreliableTransport struct {
	// Hold the configuration for the unreliable transport.
	configuration proletariat.Configuration

	// Primitive for sending messages in a unreliable way.
	comm proletariat.Communication

	// Used to resolve participant addresses.
	oracle types.Oracle

	// Channel to publish the received data.
	producer chan types.Message

	// Transport context for bounding the lifetime.
	ctx context.Context

	// Used to close the transport.
	cancel context.CancelFunc

	log types.Logger
}

func NewUnreliableTransport(peer *types.PeerConfiguration, log types.Logger, oracle types.Oracle) (Transport, error) {
	ctx, cancel := context.WithCancel(peer.Ctx)
	conf := proletariat.Configuration{
		Address: proletariat.Address(peer.Address),
		Timeout: peer.ActionTimeout,
		Ctx:     ctx,
	}
	comm, err := proletariat.NewCommunication(conf)
	if err != nil {
		cancel()
		return nil, err
	}
	ut := &UnreliableTransport{
		configuration: conf,
		comm:          comm,
		oracle:        oracle,
		producer:      make(chan types.Message),
		ctx:           ctx,
		cancel:        cancel,
		log:           log,
	}
	helper.InvokerInstance().Spawn(comm.Start)
	helper.InvokerInstance().Spawn(ut.poll)
	return ut, err
}

func (u *UnreliableTransport) dispatch(message types.Message, partition types.Partition) error {
	data, err := json.Marshal(message)
	if err != nil {
		u.log.Errorf("failed marshaling message. %#v", err)
		return err
	}
	for _, address := range u.oracle.ResolveByPartition(partition) {
		if err = u.comm.Send(proletariat.Address(address), data); err != nil {
			return err
		}
	}
	return nil
}

func (u *UnreliableTransport) Broadcast(message types.Message) error {
	for _, partition := range message.Destination {
		if err := u.dispatch(message, partition); err != nil {
			u.log.Errorf("failed sending %#v. %#v", message, err)
			return err
		}
	}
	return nil
}

func (u *UnreliableTransport) Unicast(message types.Message, partition types.Partition) error {
	return u.dispatch(message, partition)
}

func (u *UnreliableTransport) Listen() <-chan types.Message {
	return u.producer
}

func (u *UnreliableTransport) Close() error {
	u.cancel()
	return u.comm.Close()
}

func (u *UnreliableTransport) poll() {
	for {
		select {
		case <-u.ctx.Done():
			return
		case datagram, ok := <-u.comm.Receive():
			if !ok {
				return
			}
			u.consume(datagram.Data.Bytes(), datagram.Err)
		}
	}
}

func (u *UnreliableTransport) consume(data []byte, err error) {
	if err != nil {
		u.log.Errorf("failed consuming message. %v", err)
		return
	}

	if data == nil {
		u.log.Warn("received empty message")
		return
	}

	var m types.Message
	if err := json.Unmarshal(data, &m); err != nil {
		u.log.Errorf("failed unmarshalling message %#v. %v", data, err)
		return
	}

	ctx, cancel := context.WithTimeout(u.ctx, u.configuration.Timeout)
	defer cancel()
	select {
	case <-ctx.Done():
		u.log.Warnf("%s took to long consuming. %#v", u.configuration.Address, m)
		return
	case u.producer <- m:
		return
	}
}
