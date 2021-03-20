package core

import (
	"context"
	"encoding/json"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/relt/pkg/relt"
	"github.com/prometheus/common/log"
	"time"
)

// An instance of the Transport interface that
// provides the required reliable reliableTransport primitives.
type ReliableTransport struct {
	// Transport logger.
	log types.Logger

	// Reliable reliableTransport.
	relt *relt.Relt

	// Channel to publish the receiving messages.
	producer chan types.Message

	// The reliableTransport context.
	context context.Context

	// The finish function to closing the reliableTransport.
	finish context.CancelFunc

	// Timeout when applying async actions.
	timeout time.Duration

	partition string
}

// Create a new instance of the reliableTransport interface.
func NewReliableTransport(peer *types.PeerConfiguration, log types.Logger) (Transport, error) {
	conf := relt.DefaultReltConfiguration()
	conf.Name = string(peer.Name)
	conf.Exchange = relt.GroupAddress(peer.Partition)
	conf.DefaultTimeout = peer.ActionTimeout
	r, err := relt.NewRelt(*conf)
	if err != nil {
		return nil, err
	}
	ctx, done := context.WithCancel(peer.Ctx)
	t := &ReliableTransport{
		log:       log,
		relt:      r,
		producer:  make(chan types.Message),
		partition: string(peer.Partition),
		context:   ctx,
		finish:    done,
		timeout:   peer.ActionTimeout,
	}
	return t.waitTransportReady()
}

func (r *ReliableTransport) waitTransportReady() (Transport, error) {
	wait := make(chan error)
	InvokerInstance().Spawn(func() {
		r.poll(wait)
	})
	err := <-wait
	if err != nil {
		r.finish()
		return nil, err
	}
	return r, nil
}

func (r *ReliableTransport) apply(message types.Message, partition types.Partition) error {
	data, err := json.Marshal(message)
	if err != nil {
		log.Errorf("failed marshalling unicast message %#v. %v", message, err)
	}

	m := relt.Send{
		Address: relt.GroupAddress(partition),
		Data:    data,
	}
	return r.relt.Broadcast(r.context, m)
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Broadcast(message types.Message) error {
	for _, partition := range message.Destination {
		if err := r.apply(message, partition); err != nil {
			r.log.Errorf("reliable failed sending %#v. %v", message, err)
			return err
		}
	}
	return nil
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Unicast(message types.Message, partition types.Partition) error {
	return r.apply(message, partition)
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Listen() <-chan types.Message {
	return r.producer
}

// ReliableTransport implements Transport interface.
func (r *ReliableTransport) Close() {
	r.finish()
	if err := r.relt.Close(); err != nil {
		r.log.Errorf("failed stopping reliableTransport. %#v", err)
	}
}

// This method will keep polling until
// the reliableTransport context cancelled.
// The messages that arrives through the underlying
// reliableTransport channel will be sent to the consume
// method to be parsed and publish to the listeners.
func (r ReliableTransport) poll(ready chan<- error) {
	listener, err := r.relt.Consume()
	ready <- err
	for {
		select {
		case <-r.context.Done():
			return
		case recv, ok := <-listener:
			if !ok {
				return
			}
			r.consume(recv.Data, recv.Error)
		}
	}
}

// Consume will receive a message from the reliableTransport
// and will parse into a valid object to be consumed
// by the channel listener.
func (r *ReliableTransport) consume(data []byte, err error) {
	if err != nil {
		r.log.Errorf("failed consuming message at %s. %v", r.partition, err)
		return
	}

	if data == nil {
		r.log.Warnf("received empty message at %s", r.partition)
		return
	}

	var m types.Message
	if err := json.Unmarshal(data, &m); err != nil {
		r.log.Errorf("failed unmarshalling message %#v. %v", data, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.context, r.timeout)
	defer cancel()
	select {
	case <-ctx.Done():
		r.log.Warnf("%s took to long consuming. %#v", r.partition, m)
		return
	case r.producer <- m:
		return
	}
}
