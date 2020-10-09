package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"testing"
	"time"
)

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

	go func() {
		select {
		case v := <-ch:
			if v.Identifier != msg.Identifier {
				t.Errorf("Expected %s, received %s", msg.Identifier, v.Identifier)
			}
		case <-time.After(time.Second):
			t.Errorf("1 second without notification")
		}
	}()
	q.Push(msg)
}
