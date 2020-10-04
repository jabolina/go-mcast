package core

import (
	"context"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// Interface to deliver messages.
type Deliverable interface {
	// Commit the given message on the state machine.
	Commit(message types.Message) types.Response
}

// A struct that is able to deliver message from the protocol.
// The messages will be committed on the peer state machine
// and a notification will be generated,
type Deliver struct {
	// Parent context of the delivery.
	// The parent who instantiate the delivery is the peer that
	// relies inside a partition, so for each peer will exists a
	// deliver instance.
	// When the peer is shutdown, also will be shutdown the deliver.
	ctx context.Context

	// Conflict relationship to order the messages.
	conflict types.ConflictRelationship

	// The peer state machine.
	sm types.StateMachine

	// Deliver logger.
	log types.Logger
}

// Creates a new instance of the Deliverable interface.
func NewDeliver(ctx context.Context, log types.Logger, conflict types.ConflictRelationship, storage types.Storage) (Deliverable, error) {
	sm := types.NewStateMachine(storage)
	if err := sm.Restore(); err != nil {
		return nil, err
	}
	d := &Deliver{
		ctx:      ctx,
		conflict: conflict,
		sm:       sm,
		log:      log,
	}
	return d, nil
}

// Commit the message on the peer state machine.
// After the commit a notification is sent through the commit channel.
func (d Deliver) Commit(m types.Message) types.Response {
	res := types.Response{
		Success:    false,
		Identifier: m.Identifier,
		Data:       nil,
		Extra:      nil,
		Failure:    nil,
	}
	d.log.Debugf("commit request %#v", m)
	entry := &types.Entry{
		Operation:      m.Content.Operation,
		Identifier:     m.Identifier,
		Key:            m.Content.Key,
		FinalTimestamp: m.Timestamp,
		Data:           m.Content.Content,
		Extensions:     m.Content.Extensions,
	}
	commit, err := d.sm.Commit(entry)
	if err != nil {
		d.log.Errorf("failed to commit %#v. %v", m, err)
		res.Success = false
		res.Failure = err
	} else {
		switch c := commit.(type) {
		case *types.Entry:
			res.Success = true
			res.Data = c.Data
			res.Extra = c.Extensions
		default:
			res.Success = false
			res.Failure = fmt.Errorf("commit unknown response. %#v", c)
		}
	}
	return res
}
