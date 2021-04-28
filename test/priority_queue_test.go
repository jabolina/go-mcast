package test

import (
	"context"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/hpq"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test/util"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueue_ShouldNotifyAboutHead(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	validation := func(element hpq.QueueElement) bool {
		return true
	}
	q := hpq.NewPriorityQueue(context.TODO(), ch, validation)

	msg := types.Message{
		Timestamp:  0,
		Identifier: types.UID(helper.GenerateUID()),
	}

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		select {
		case e := <-ch:
			if e.Id() != msg.Identifier {
				t.Errorf("Expected %s, received %s", msg.Identifier, e.Id())
			}
		case <-time.After(util.DefaultTestTimeout):
			t.Errorf("5 seconds without notification")
		}
	}()
	q.Push(wrapMessage(msg))
	group.Wait()
}

func TestQueue_ShouldSetLowestOnHead(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	done := make(chan bool)
	h := util.NewHolder()
	q := hpq.NewPriorityQueue(context.TODO(), ch, func(element hpq.QueueElement) bool {
		return true
	})

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		for {
			select {
			case <-done:
				return
			case v := <-ch:
				h.Set(unwrapMessage(v).Timestamp)
			}
		}
	}()

	// Insert from 5 to 0. This will trigger 5 head changes.
	for i := 5; i >= 0; i -= 1 {
		msg := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(wrapMessage(msg))
	}

	// Insert from 6 to 10, this will not change the head.
	for i := 6; i <= 10; i += 1 {
		msg := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(wrapMessage(msg))
	}

	// Should wait so we know that the channel was called.
	time.Sleep(50 * time.Millisecond)

	// After sending all requests, the head must be with Timestamp 0.
	if h.Get() != 0 {
		t.Errorf("Expected head 0, found %d", h.Get())
	}

	// Reset the value for the Holder.
	h.Set(math.MaxUint64)
	m1 := unwrapMessage(q.Pop())
	if m1.Timestamp != 0 {
		t.Errorf("Expected timestamp 0, found %d", m1.Timestamp)
	}

	// Should wait so we know that the channel was called.
	time.Sleep(50 * time.Millisecond)

	// Since we removed the element at the head, now the value must be 1.
	if h.Get() != 1 {
		t.Errorf("Expected head 1, found %d", h.Get())
	}

	done <- true
	group.Wait()
}

func TestQueue_ShouldHaveSmallestConcurrent(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	done := make(chan bool)
	h := util.NewHolder()
	q := hpq.NewPriorityQueue(context.TODO(), ch, func(element hpq.QueueElement) bool {
		return true
	})

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		for {
			select {
			case <-done:
				return
			case v := <-ch:
				h.Set(unwrapMessage(v).Timestamp)
			}
		}
	}()

	go func() {
		// Insert from 5 to 0. This will trigger 5 head changes.
		for i := 5; i >= 0; i -= 1 {
			msg := types.Message{
				Timestamp:  uint64(i),
				Identifier: types.UID(helper.GenerateUID()),
			}
			go q.Push(wrapMessage(msg))
		}
	}()

	go func() {
		// Insert from 6 to 10, this will not change the head.
		for i := 6; i <= 10; i += 1 {
			msg := types.Message{
				Timestamp:  uint64(i),
				Identifier: types.UID(helper.GenerateUID()),
			}
			go q.Push(wrapMessage(msg))
		}
	}()

	// Should wait so we know that the channel was called.
	time.Sleep(100 * time.Millisecond)

	// After sending all requests, the head must be with Timestamp 0.
	if h.Get() != 0 {
		t.Errorf("Expected head 0, found %d", h.Get())
	}

	// Reset the value for the Holder.
	h.Set(math.MaxUint64)
	m1 := unwrapMessage(q.Pop())
	if m1.Timestamp != 0 {
		t.Errorf("Expected timestamp 0, found %d", m1.Timestamp)
	}

	// Should wait so we know that the channel was called.
	time.Sleep(100 * time.Millisecond)

	// Since we removed the element at the head, now the value must be 1.
	if h.Get() != 1 {
		t.Errorf("Expected head 1, found %d", h.Get())
	}

	done <- true
	group.Wait()
}

func TestQueue_ShouldEnqueueAndDequeue(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	done := make(chan bool)
	q := hpq.NewPriorityQueue(context.TODO(), ch, func(element hpq.QueueElement) bool {
		return true
	})
	canAppend := util.NewHolder()
	read := util.NewSafeSlice()

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		for {
			select {
			case <-done:
				return
			case e := <-ch:
				if canAppend.Get() == 1 {
					v := unwrapMessage(e)
					read.Add(util.MessageId{
						Id:        v.Identifier,
						Timestamp: v.Timestamp,
					})
				}
			}
		}
	}()

	for i := 10; i >= 0; i -= 1 {
		msg := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(fmt.Sprintf("%d", i)),
		}
		go q.Push(wrapMessage(msg))
	}

	// Should wait so we know that the channel was called.
	time.Sleep(100 * time.Millisecond)
	canAppend.Set(1)

	// Remove all elements from the queue.
	// The values were inserted in the desc order but
	// must be retrieved on the asc order.
	for i := 0; i <= 10; i++ {
		recv := unwrapMessage(q.Pop())
		if recv.Timestamp != uint64(i) {
			t.Errorf("Expected timestamp %d, found %d", i, recv.Timestamp)
		}
	}

	// Should wait so we know that the channel was called and the mutex retention is done.
	time.Sleep(200 * time.Millisecond)

	// After inserted all values in the queue. The first head - value 0 - when we start to
	// pop values will not be added to the read slice.
	if read.Len() != 10 {
		t.Errorf("Expected 10 items, found %d", read.Len())
	}

	// The change on the head also happened on the asc order, from 1 to 10.
	// The value 0 was the first head and was not added to the read slice.
	for i := 1; i < read.Len(); i++ {
		if read.Get(i-1).Timestamp != uint64(i) {
			t.Errorf("expected %d at %d, found %d", i, i-1, read.Get(i-1).Timestamp)
		}
	}

	done <- true
	group.Wait()
}

func TestQueue_ShouldEnqueueAndDequeueBasedOnId(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	done := make(chan bool)
	q := hpq.NewPriorityQueue(context.TODO(), ch, func(element hpq.QueueElement) bool {
		return true
	})
	canAppend := util.NewHolder()
	read := util.NewSafeSlice()

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		for {
			select {
			case <-done:
				return
			case e := <-ch:
				if canAppend.Get() == 1 {
					v := unwrapMessage(e)
					read.Add(util.MessageId{
						Id:        v.Identifier,
						Timestamp: v.Timestamp,
					})
				}
			}
		}
	}()

	for i := 9; i >= 0; i-- {
		msg := types.Message{
			Timestamp:  uint64(1),
			Identifier: types.UID(fmt.Sprintf("%d", i)),
		}
		go q.Push(wrapMessage(msg))
	}

	// Should wait so we know that the channel was called.
	time.Sleep(50 * time.Millisecond)
	canAppend.Set(1)

	// Remove all elements from the queue.
	// The values were inserted in the desc order but
	// must be retrieved on the asc order.
	for i := 0; i <= 9; i++ {
		recv := unwrapMessage(q.Pop())
		id := types.UID(fmt.Sprintf("%d", i))
		if recv.Identifier != id {
			t.Errorf("Expected id %s, found %s", id, recv.Identifier)
		}
	}

	// Should wait so we know that the channel was called and the mutex retention is done.
	time.Sleep(50 * time.Millisecond)

	// After inserted all values in the queue. The first head - value 0 - when we start to
	// pop values will not be added to the read slice.
	if read.Len() != 9 {
		t.Errorf("Expected 9 items, found %d", read.Len())
	}

	// The change on the head also happened on the asc order, from 1 to 9.
	// The value 0 was the first head and was not added to the read slice.
	for i := 1; i < read.Len(); i++ {
		id := types.UID(fmt.Sprintf("%d", i))
		if read.Get(i-1).Id != id {
			t.Errorf("expected %s at %d, found %s", id, i, read.Get(i-1).Id)
		}
	}

	done <- true
	group.Wait()
}

func TestQueue_NotificationWillClearQueue(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	done := make(chan bool)
	read := util.NewSafeSlice()
	q := hpq.NewPriorityQueue(context.TODO(), ch, func(element hpq.QueueElement) bool {
		return unwrapMessage(element).State == types.S3
	})

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		for {
			select {
			case <-done:
				return
			case e := <-ch:
				v := unwrapMessage(e)
				read.Add(util.MessageId{
					Id:        v.Identifier,
					Timestamp: v.Timestamp,
				})
				go q.Pop()
			}
		}
	}()

	var added []types.Message

	// Insert from 10 to 1. The head will change for every value.
	// But since we are not on state S3, the notification will not be called.
	for i := 10; i >= 0; i -= 1 {
		msg := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(wrapMessage(msg))
		added = append(added, msg)
	}

	time.Sleep(50 * time.Millisecond)
	if read.Len() != 0 {
		t.Fatalf("should not have notifications.")
	}

	for _, item := range added {
		item.State = types.S3
		q.Push(wrapMessage(item))
	}

	time.Sleep(50 * time.Millisecond)
	if read.Len() != 11 {
		t.Errorf("Expected 11 items, found %d", read.Len())
	}

	for i := 0; i < read.Len(); i++ {
		if read.Get(i).Timestamp != uint64(i) {
			t.Errorf("expected %d at %d, found %d", i, i, read.Get(i).Timestamp)
		}
	}

	done <- true
	group.Wait()
}

func TestQueue_ShouldKeepSmallestOnHead(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	validation := func(element hpq.QueueElement) bool {
		return unwrapMessage(element).State == types.S3
	}
	q := hpq.NewPriorityQueue(context.TODO(), ch, validation)
	theOriginal := types.Message{
		Timestamp:  0,
		Identifier: types.UID(helper.GenerateUID()),
	}

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		select {
		case v := <-ch:
			if v.Id() != theOriginal.Identifier {
				t.Errorf("Expected %s, received %s", theOriginal.Identifier, v.Id())
			}
		case <-time.After(time.Second):
			t.Errorf("no notification received")
		}
	}()

	q.Push(wrapMessage(theOriginal))

	for i := 1; i < 20; i++ {
		anyMessage := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(wrapMessage(anyMessage))
	}

	theOriginal.State = types.S3
	q.Push(wrapMessage(theOriginal))

	group.Wait()
}

func TestQueue_ShouldNotifyCorrectlyWhenRemovedArbitraryItems(t *testing.T) {
	ch := make(chan hpq.QueueElement)
	validation := func(element hpq.QueueElement) bool {
		return unwrapMessage(element).State == types.S3
	}
	ctx, cancel := context.WithCancel(context.TODO())
	notificationCounter := int32(0)
	q := hpq.NewPriorityQueue(context.TODO(), ch, validation)
	willBeRemoved := types.Message{
		Timestamp:  0,
		Identifier: "2-removed-uid",
	}
	theOriginal := types.Message{
		Timestamp:  0,
		Identifier: "1-the-one-uid",
	}
	shouldNotNotify := types.Message{
		Timestamp:  1,
		Identifier: "nope-uid",
	}

	group := sync.WaitGroup{}
	group.Add(1)
	go func() {
		defer group.Done()
		select {
		case v := <-ch:
			t.Logf("notification for %s", v.Id())
			atomic.AddInt32(&notificationCounter, 0x1)
		case <-ctx.Done():
			return
		}
	}()

	q.Push(wrapMessage(willBeRemoved))
	q.Push(wrapMessage(theOriginal))
	q.Push(wrapMessage(shouldNotNotify))

	shouldNotNotify.State = types.S3
	q.Push(wrapMessage(shouldNotNotify))

	q.Remove(wrapMessage(willBeRemoved))

	theOriginal.State = types.S3
	q.Push(wrapMessage(theOriginal))

	time.Sleep(150 * time.Millisecond)
	notifications := atomic.LoadInt32(&notificationCounter)
	if notifications != 0x1 {
		t.Errorf("expected %d notifications, found %d", 1, notifications)
	}

	cancel()
	group.Wait()
}
