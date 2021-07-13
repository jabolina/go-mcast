package protocol

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/output"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"io"
)

// The Protocol interface is just the entry point to interact with the algorithm.
// This interface exists only to group together the message processing and the
// content reading.
// The actual algorithm does not include any reading committed values, but for
// development is important to verify if the algorithm is working correctly.
type Protocol interface {
	io.Closer

	// Process will interact with the algorithm to process the message and
	// return the next algorithm step.
	Process(*types.Message) Step

	// Read will dump the values that are present in the WAL.
	Read() types.Response
}

// Manager is the concrete Protocol structure.
type Manager struct {
	// Holds the algorithm, used to process messages.
	algorithm *Algorithm

	// Holds the peer logger, this will be used for reads only,
	// all writes are done from the state machine when a commit is applied.
	wal output.Log
}

func NewProtocol(configuration types.PeerConfiguration, invoker helper.Invoker) (Protocol, error) {
	wal := output.NewLogStructure(configuration.Storage)
	deliver, err := output.NewDeliver(string(configuration.Name), wal)
	if err != nil {
		configuration.Cancel()
		return nil, err
	}

	p := &Manager{
		algorithm: NewAlgorithm(configuration, deliver, invoker),
		wal:       wal,
	}
	return p, nil
}

// Close implements the io.Closer interface.
func (m *Manager) Close() error {
	return m.algorithm.Close()
}

// Process implements the Protocol interface.
// Delegates the message processing to the actual algorithm implementation.
func (m *Manager) Process(message *types.Message) Step {
	return m.algorithm.ReceiveMessage(message)
}

// Read implements the Protocol interface.
// This will dump the values present on the log.
func (m *Manager) Read() types.Response {
	res := types.Response{
		Success: false,
		Data:    nil,
		Failure: nil,
	}

	data, err := m.wal.Dump()
	if err != nil {
		res.Success = false
		res.Data = nil
		res.Failure = err
		return res
	}

	res.Success = true
	res.Failure = nil
	for _, message := range data {
		res.Data = append(res.Data, message.Content)
	}
	return res
}
