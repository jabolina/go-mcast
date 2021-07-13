package protocol

import (
	"context"
	"github.com/jabolina/go-mcast/pkg/mcast/definition"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/output"
	"github.com/jabolina/go-mcast/pkg/mcast/protocol"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test/util"
	"testing"
)

func createTestingProtocol(ctx context.Context, t *testing.T) *protocol.Algorithm {
	previousSet := protocol.NewPreviousSet(definition.AlwaysConflict{})
	deliverable, err := output.NewDeliver("testing-deliverable", output.NewLogStructure(output.NewDefaultStorage()))
	if err != nil {
		t.Fatalf("Failed creating deliver. %#v", err)
		return nil
	}

	return protocol.NewAlgorithm(make(chan types.Response), ctx, previousSet, deliverable, util.NewInvoker())
}

func Test_ReturnNextStepForMessageS0(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := createTestingProtocol(ctx, t)
	msg := &types.Message{
		Header: types.ProtocolHeader{
			Type: types.ABCast,
		},
		Identifier:  types.UID(helper.GenerateUID()),
		State:       types.S0,
		Destination: []types.Partition{types.Partition(helper.GenerateUID()), types.Partition(helper.GenerateUID())},
	}
	messageExists := func(message types.Message) bool {
		return msg.Identifier == message.Identifier
	}

	expectedNext := protocol.ExchangeAll
	actualStep := p.ReceiveMessage(msg)

	if expectedNext != actualStep {
		t.Errorf("Expected step %d found %d", expectedNext, actualStep)
	}

	if msg.State != types.S1 {
		t.Errorf("Expected state %d found %d", types.S1, msg.State)
	}

	if !p.Mem.Exists(messageExists) {
		t.Errorf("Expected message to exists in Mem structure")
	}
}

func Test_CompleteStepsUntilDeliveringMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := createTestingProtocol(ctx, t)
	msg := &types.Message{
		Header: types.ProtocolHeader{
			Type: types.ABCast,
		},
		Identifier:  types.UID(helper.GenerateUID()),
		State:       types.S0,
		From:        types.Partition(helper.GenerateUID()),
		Destination: []types.Partition{types.Partition(helper.GenerateUID()), types.Partition(helper.GenerateUID())},
	}
	messageExists := func(message types.Message) bool {
		return msg.Identifier == message.Identifier
	}

	var actualStep = p.ReceiveMessage(msg)
	if protocol.ExchangeAll != actualStep {
		t.Errorf("Expected step %d found %d", protocol.ExchangeAll, actualStep)
	}

	if msg.State != types.S1 {
		t.Errorf("Expected state %d found %d", types.S1, msg.State)
	}

	if !p.Mem.Exists(messageExists) {
		t.Errorf("Expected message to exists in Mem structure")
	}

	msg.Header.Type = types.Network
	actualStep = p.ReceiveMessage(msg)
	if protocol.NoOp != actualStep {
		t.Errorf("Expected step %d found %d", protocol.NoOp, actualStep)
	}

	// Verifying that if a message come again from the same partition the protocol
	// does not try to go to another step.
	actualStep = p.ReceiveMessage(msg)
	if protocol.NoOp != actualStep {
		t.Errorf("Expected step %d found %d", protocol.NoOp, actualStep)
	}

	msg.From = types.Partition(helper.GenerateUID())
	actualStep = p.ReceiveMessage(msg)
	if protocol.Ended != actualStep {
		t.Errorf("Expected step %d found %d", protocol.Ended, actualStep)
	}

	if msg.State != types.S3 {
		t.Errorf("Expected state %d found %d", types.S3, msg.State)
	}

	if !p.Mem.Exists(messageExists) {
		t.Errorf("Expected message to exists in Mem structure")
	}
}

func Test_ReturnsS2WhenNeedToSynchronize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := createTestingProtocol(ctx, t)
	msg := &types.Message{
		Header: types.ProtocolHeader{
			Type: types.ABCast,
		},
		Identifier:  types.UID(helper.GenerateUID()),
		State:       types.S0,
		From:        types.Partition(helper.GenerateUID()),
		Destination: []types.Partition{types.Partition(helper.GenerateUID()), types.Partition(helper.GenerateUID())},
	}
	messageExists := func(message types.Message) bool {
		return msg.Identifier == message.Identifier
	}

	var actualStep = p.ReceiveMessage(msg)
	if protocol.ExchangeAll != actualStep {
		t.Errorf("Expected step %d found %d", protocol.ExchangeAll, actualStep)
	}

	if msg.State != types.S1 {
		t.Errorf("Expected state %d found %d", types.S1, msg.State)
	}

	if !p.Mem.Exists(messageExists) {
		t.Errorf("Expected message to exists in Mem structure")
	}

	msg.Header.Type = types.Network
	msg.Timestamp = uint64(2)
	actualStep = p.ReceiveMessage(msg)
	if protocol.NoOp != actualStep {
		t.Errorf("Expected step %d found %d", protocol.NoOp, actualStep)
	}

	msg.From = types.Partition(helper.GenerateUID())
	msg.Timestamp = uint64(1)
	actualStep = p.ReceiveMessage(msg)
	if protocol.ExchangeInternal != actualStep {
		t.Errorf("Expected step %d found %d", protocol.ExchangeInternal, actualStep)
	}

	if msg.State != types.S2 {
		t.Errorf("Expected state %d found %d", types.S2, msg.State)
	}

	if !p.Mem.Exists(messageExists) {
		t.Errorf("Expected message to exists in Mem structure")
	}
}
