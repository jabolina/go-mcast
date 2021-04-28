package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/hpq"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"go.uber.org/goleak"
	"testing"
)

func wrapMessage(message types.Message) hpq.MessageWrapper {
	return hpq.MessageWrapper{Message: message}
}

func unwrapMessage(i interface{}) types.Message {
	return i.(hpq.MessageWrapper).Message
}

func Test_ShouldInsertAndReadSuccessfully(t *testing.T) {
	defer goleak.VerifyNone(t)

	h := hpq.NewHeap()

	for i := 9; i >= 0; i-- {
		m := types.Message{
			Identifier: types.UID(helper.GenerateUID()),
			Timestamp:  uint64(i),
		}
		h.Insert(wrapMessage(m))
	}

	for i := 0; i < 10; i++ {
		m := h.Pop()
		if m == nil {
			t.Errorf("should not be nil at %d", i)
		} else {
			if unwrapMessage(m).Timestamp != uint64(i) {
				t.Errorf("should have ts %d, found %d", i, m.(types.Message).Timestamp)
			}
		}
	}

	m := h.Pop()
	if m != nil {
		t.Errorf("should be nil")
	}
}

func Test_ShouldInsertAndUpdate(t *testing.T) {
	defer goleak.VerifyNone(t)

	h := hpq.NewHeap()

	initial := types.Message{
		Identifier: types.UID(helper.GenerateUID()),
		Timestamp:  0,
	}
	other := types.Message{
		Identifier: types.UID(helper.GenerateUID()),
		Timestamp:  1,
	}

	h.Insert(wrapMessage(initial))
	h.Insert(wrapMessage(other))

	curr := h.Peek()
	if curr == nil {
		t.Errorf("head is nil")
	}

	if unwrapMessage(curr).Identifier != initial.Identifier {
		t.Errorf("should be %#v but was %#v", initial, curr)
	}

	initial.Timestamp = 2
	h.Insert(wrapMessage(initial))

	curr = h.Peek()
	if curr == nil {
		t.Errorf("head is nil")
	}

	if unwrapMessage(curr).Identifier != other.Identifier {
		t.Errorf("should be %#v but was %#v", other, curr)
	}
}

func Test_InsertAndRemoveBackwards(t *testing.T) {
	defer goleak.VerifyNone(t)

	var msgs []types.Message

	h := hpq.NewHeap()

	for i := 9; i >= 0; i-- {
		m := types.Message{
			Identifier: types.UID(helper.GenerateUID()),
			Timestamp:  uint64(i),
		}
		msgs = append(msgs, m)
		h.Insert(wrapMessage(m))
	}

	for _, msg := range msgs {
		curr := h.Remove(wrapMessage(msg))
		if curr == nil {
			t.Errorf("current should not be nil")
			continue
		}

		if unwrapMessage(curr).Identifier != msg.Identifier {
			t.Errorf("expected %#v but found %#v", msg, curr)
		}
	}

	m := h.Pop()
	if m != nil {
		t.Errorf("should be nil")
	}
}

func Test_ShouldInsertAndReadAllElements(t *testing.T) {
	defer goleak.VerifyNone(t)

	var msgs []types.Message

	h := hpq.NewHeap()

	for i := 9; i >= 0; i-- {
		m := types.Message{
			Identifier: types.UID(helper.GenerateUID()),
			Timestamp:  uint64(i),
		}
		msgs = append(msgs, m)
		h.Insert(wrapMessage(m))
	}

	curr := h.Values()
	if len(curr) != len(msgs) {
		t.Errorf("should have size %d, but found %d", len(msgs), len(curr))
	}

	for _, msg := range curr {
		if !contains(unwrapMessage(msg), msgs) {
			t.Errorf("unknown message %#v", msg)
		}
	}
}

func contains(msg types.Message, msgs []types.Message) bool {
	for _, message := range msgs {
		if message.Identifier == msg.Identifier {
			return true
		}
	}
	return false
}
