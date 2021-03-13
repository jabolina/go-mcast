package mcast

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
)

type IMulticast interface {
	io.Closer

	Write(types.Request) <-chan types.Response

	Read() types.Response
}

type Multicast struct {
	peer          core.PartitionPeer
	configuration *types.Configuration
}

func NewGenericMulticast(configuration *types.Configuration) (IMulticast, error) {
	if err := configuration.IsValid(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	peerConfiguration := &types.PeerConfiguration{
		Name:      configuration.Name,
		Partition: configuration.Partition,
		Address:   configuration.Address,
		Version:   configuration.Version,
		Conflict:  configuration.Conflict,
		Storage:   configuration.Storage,
		Ctx:       ctx,
		Cancel:    cancel,
	}
	peer, err := core.NewPeer(peerConfiguration, configuration.Oracle, configuration.Logger)
	if err != nil {
		cancel()
		return nil, err
	}
	m := &Multicast{
		peer:          peer,
		configuration: configuration,
	}
	return m, nil
}

func (m *Multicast) Close() error {
	m.peer.Stop()
	return nil
}

func (m *Multicast) Write(request types.Request) <-chan types.Response {
	id := types.UID(helper.GenerateUID())
	message := types.Message{
		Header: types.ProtocolHeader{
			ProtocolVersion: m.configuration.Version,
			Type:            types.Initial,
		},
		Identifier: id,
		Content: types.DataHolder{
			Operation:  types.Command,
			Content:    request.Value,
			Extensions: request.Extra,
		},
		State:       types.S0,
		Timestamp:   0,
		Destination: request.Destination,
		From:        m.configuration.Partition,
	}
	return m.peer.Command(message)
}

func (m *Multicast) Read() types.Response {
	return m.peer.FastRead()
}
