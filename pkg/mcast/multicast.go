package mcast

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/network"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
)

type IMulticast interface {
	io.Closer

	Write(types.Request) error

	Read() types.Response

	Listen() <-chan types.Response
}

type Multicast struct {
	peer          network.Network
	configuration *types.Configuration
	commit        chan types.Response
}

func NewGenericMulticast(configuration *types.Configuration) (IMulticast, error) {
	if err := configuration.IsValid(); err != nil {
		return nil, err
	}
	commitChan := make(chan types.Response)
	ctx, cancel := context.WithCancel(context.Background())
	peerConfiguration := &types.PeerConfiguration{
		Name:          configuration.Name,
		Partition:     configuration.Partition,
		Address:       configuration.Address,
		Version:       configuration.Version,
		Conflict:      configuration.Conflict,
		Storage:       configuration.Storage,
		Ctx:           ctx,
		Cancel:        cancel,
		Commit:        commitChan,
		ActionTimeout: configuration.DefaultTimeout,
	}
	peer, err := network.NewNetworkManager(peerConfiguration, configuration.Oracle, configuration.Logger, helper.InvokerInstance())
	if err != nil {
		cancel()
		return nil, err
	}
	m := &Multicast{
		peer:          peer,
		configuration: configuration,
		commit:        commitChan,
	}
	return m, nil
}

func (m *Multicast) Close() error {
	return m.peer.Close()
}

func (m *Multicast) Write(request types.Request) error {
	id := types.UID(helper.GenerateUID())
	message := types.Message{
		Header: types.ProtocolHeader{
			ProtocolVersion: m.configuration.Version,
			Type:            types.ABCast,
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

func (m *Multicast) Listen() <-chan types.Response {
	return m.commit
}

func (m *Multicast) Read() types.Response {
	return m.peer.FastRead()
}
