package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"math"
	"sync"
	"testing"
	"time"
)

type holder struct {
	timestamp uint64
	mutex sync.Mutex
}

func (h *holder) Set(value uint64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.timestamp = value
}

func (h *holder) Get() uint64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.timestamp
}

func TestQueue_ShouldNotifyAboutHead(t *testing.T) {
	ch := make(chan types.Message)
	validation := func(message types.Message) bool {
		return true
	}
	q := core.NewPriorityQueue(ch, validation)

	msg := types.Message{
		Timestamp: 0,
		Identifier: types.UID(helper.GenerateUID()),
	}

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		select {
		case v := <-ch:
			if v.Identifier != msg.Identifier {
				t.Errorf("Expected %s, received %s", msg.Identifier, v.Identifier)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("5 seconds without notification")
		}
	}()
	q.Push(msg)
	group.Wait()
}

func TestQueue_ShouldSetLowestOnHead(t *testing.T) {
	ch := make(chan types.Message)
	done := make(chan bool)
	validation := func(message types.Message) bool {
		return true
	}
	q := core.NewPriorityQueue(ch, validation)
	h := &holder{
		timestamp: math.MaxUint64,
		mutex:     sync.Mutex{},
	}

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		for {
			select {
			case <-done:
				return
			case v := <-ch:
				if v.Timestamp < h.Get() {
					h.Set(v.Timestamp)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("5 seconds without notification")
			}
		}
	}()

	for i := 5; i >= 0; i -= 1 {
		msg := types.Message{
			Timestamp: uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(msg)
	}

	done <- true
	group.Wait()
	if h.Get() > 0 {
		t.Errorf("Expected 0, found %d", h.Get())
	}
}
