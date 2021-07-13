package output

import (
	"errors"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

var (
	ErrCommandUnknown = errors.New("unknown command applied into state machine")
)

// Deliverable interface to deliver messages.
type Deliverable interface {
	// Commit the given message on the state machine.
	Commit(message types.Message, isGenericDelivery bool) types.Response
}

// Deliver is a struct that is able to deliver message from the protocol.
// The messages will be committed on the peer state machine
// and a notification will be generated,
type Deliver struct {
	name string

	// The peer state machine.
	sm StateMachine
}

// NewDeliver creates a new instance of the Deliverable interface.
func NewDeliver(name string, logStructure Log) (Deliverable, error) {
	sm := NewStateMachine(logStructure)
	if err := sm.Restore(); err != nil {
		return nil, err
	}
	d := &Deliver{
		name: name,
		sm:   sm,
	}
	return d, nil
}

func (d Deliver) Commit(m types.Message, isGenericDelivery bool) types.Response {
	err := d.sm.Commit(m, isGenericDelivery)

	res := types.Response{
		Success: false,
		Data:    nil,
		Failure: nil,
	}

	if err != nil {
		res.Success = false
		res.Failure = err
		return res
	}

	res.Success = true
	res.Failure = nil
	switch m.Content.Operation {
	case types.Command:
		holder := types.DataHolder{
			Meta: types.Meta{
				Identifier: m.Identifier,
				Timestamp:  m.Timestamp,
			},
			Operation:  types.Command,
			Content:    m.Content.Content,
			Extensions: m.Content.Extensions,
		}
		res.Data = []types.DataHolder{holder}
	case types.Query:
		messages, err := d.sm.History()
		if err != nil {
			res.Success = false
			res.Data = nil
			res.Failure = err
		} else {
			for _, message := range messages {
				res.Data = append(res.Data, message.Content)
			}
		}
	default:
		res.Success = false
		res.Data = nil
		res.Failure = ErrCommandUnknown
	}

	return res
}
