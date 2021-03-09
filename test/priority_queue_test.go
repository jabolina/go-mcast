package test

import (
	"context"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type holder struct {
	timestamp uint64
	mutex     sync.Mutex
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

type messageId struct {
	id        types.UID
	timestamp uint64
}

type safeSlice struct {
	data  []messageId
	mutex sync.Mutex
}

func (s *safeSlice) Add(value messageId) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data = append(s.data, value)
}

func (s *safeSlice) Len() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.data)
}

func (s *safeSlice) Get(index int) messageId {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.data[index]
}

func TestQueue_ShouldNotifyAboutHead(t *testing.T) {
	ch := make(chan types.Message)
	validation := func(message types.Message) bool {
		return true
	}
	q := core.NewPriorityQueue(ch, validation)

	msg := types.Message{
		Timestamp:  0,
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
	h := &holder{
		timestamp: math.MaxUint64,
		mutex:     sync.Mutex{},
	}
	q := core.NewPriorityQueue(ch, func(message types.Message) bool {
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
				h.Set(v.Timestamp)
			}
		}
	}()

	// Insert from 5 to 0. This will trigger 5 head changes.
	for i := 5; i >= 0; i -= 1 {
		msg := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(msg)
	}

	// Insert from 6 to 10, this will not change the head.
	for i := 6; i <= 10; i += 1 {
		msg := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(msg)
	}

	// Should wait so we know that the channel was called.
	time.Sleep(50 * time.Millisecond)

	// After sending all requests, the head must be with Timestamp 0.
	if h.Get() != 0 {
		t.Errorf("Expected head 0, found %d", h.Get())
	}

	// Reset the value for the holder.
	h.Set(math.MaxUint64)
	m1 := q.Pop().(types.Message)
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
	ch := make(chan types.Message)
	done := make(chan bool)
	h := &holder{
		timestamp: math.MaxUint64,
		mutex:     sync.Mutex{},
	}
	q := core.NewPriorityQueue(ch, func(message types.Message) bool {
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
				h.Set(v.Timestamp)
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
			go q.Push(msg)
		}
	}()

	go func() {
		// Insert from 6 to 10, this will not change the head.
		for i := 6; i <= 10; i += 1 {
			msg := types.Message{
				Timestamp:  uint64(i),
				Identifier: types.UID(helper.GenerateUID()),
			}
			go q.Push(msg)
		}
	}()

	// Should wait so we know that the channel was called.
	time.Sleep(100 * time.Millisecond)

	// After sending all requests, the head must be with Timestamp 0.
	if h.Get() != 0 {
		t.Errorf("Expected head 0, found %d", h.Get())
	}

	// Reset the value for the holder.
	h.Set(math.MaxUint64)
	m1 := q.Pop().(types.Message)
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
	ch := make(chan types.Message)
	done := make(chan bool)
	q := core.NewPriorityQueue(ch, func(message types.Message) bool {
		return true
	})
	canAppend := holder{
		timestamp: 0,
		mutex:     sync.Mutex{},
	}
	read := safeSlice{
		data:  []messageId{},
		mutex: sync.Mutex{},
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
				if canAppend.Get() == 1 {
					read.Add(messageId{
						id:        v.Identifier,
						timestamp: v.Timestamp,
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
		go q.Push(msg)
	}

	// Should wait so we know that the channel was called.
	time.Sleep(100 * time.Millisecond)
	canAppend.Set(1)

	// Remove all elements from the queue.
	// The values were inserted in the desc order but
	// must be retrieved on the asc order.
	for i := 0; i <= 10; i++ {
		recv := q.Pop().(types.Message)
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
		if read.Get(i-1).timestamp != uint64(i) {
			t.Errorf("expected %d at %d, found %d", i, i-1, read.Get(i-1).timestamp)
		}
	}

	done <- true
	group.Wait()
}

func TestQueue_ShouldEnqueueAndDequeueBasedOnId(t *testing.T) {
	ch := make(chan types.Message)
	done := make(chan bool)
	q := core.NewPriorityQueue(ch, func(message types.Message) bool {
		return true
	})
	canAppend := holder{
		timestamp: 0,
		mutex:     sync.Mutex{},
	}
	read := safeSlice{
		data:  []messageId{},
		mutex: sync.Mutex{},
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
				if canAppend.Get() == 1 {
					read.Add(messageId{
						id:        v.Identifier,
						timestamp: v.Timestamp,
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
		go q.Push(msg)
	}

	// Should wait so we know that the channel was called.
	time.Sleep(50 * time.Millisecond)
	canAppend.Set(1)

	// Remove all elements from the queue.
	// The values were inserted in the desc order but
	// must be retrieved on the asc order.
	for i := 0; i <= 9; i++ {
		recv := q.Pop().(types.Message)
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
		if read.Get(i-1).id != id {
			t.Errorf("expected %s at %d, found %s", id, i, read.Get(i-1).id)
		}
	}

	done <- true
	group.Wait()
}

func TestQueue_NotificationWillClearQueue(t *testing.T) {
	ch := make(chan types.Message)
	done := make(chan bool)
	read := safeSlice{
		data:  []messageId{},
		mutex: sync.Mutex{},
	}
	q := core.NewPriorityQueue(ch, func(message types.Message) bool {
		return message.State == types.S3
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
				read.Add(messageId{
					id:        v.Identifier,
					timestamp: v.Timestamp,
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
		q.Push(msg)
		added = append(added, msg)
	}

	time.Sleep(50 * time.Millisecond)
	if read.Len() != 0 {
		t.Fatalf("should not have notifications.")
	}

	for _, item := range added {
		item.State = types.S3
		q.Push(item)
	}

	time.Sleep(50 * time.Millisecond)
	if read.Len() != 11 {
		t.Errorf("Expected 11 items, found %d", read.Len())
	}

	for i := 0; i < read.Len(); i++ {
		if read.Get(i).timestamp != uint64(i) {
			t.Errorf("expected %d at %d, found %d", i, i, read.Get(i).timestamp)
		}
	}

	done <- true
	group.Wait()
}

func TestQueue_ShouldKeepSmallestOnHead(t *testing.T) {
	ch := make(chan types.Message)
	validation := func(message types.Message) bool {
		return message.State == types.S3
	}
	q := core.NewPriorityQueue(ch, validation)
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
			if v.Identifier != theOriginal.Identifier {
				t.Errorf("Expected %s, received %s", theOriginal.Identifier, v.Identifier)
			}
		case <-time.After(time.Second):
			t.Errorf("no notification received")
		}
	}()

	q.Push(theOriginal)

	for i := 1; i < 20; i++ {
		anyMessage := types.Message{
			Timestamp:  uint64(i),
			Identifier: types.UID(helper.GenerateUID()),
		}
		q.Push(anyMessage)
	}

	theOriginal.State = types.S3
	q.Push(theOriginal)

	group.Wait()
}

func TestQueue_ShouldNotifyCorrectlyWhenRemovedArbitraryItems(t *testing.T) {
	ch := make(chan types.Message)
	validation := func(message types.Message) bool {
		return message.State == types.S3
	}
	ctx, cancel := context.WithCancel(context.TODO())
	notificationCounter := int32(0)
	q := core.NewPriorityQueue(ch, validation)
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
		case v :=<-ch:
			t.Logf("notification for %s", v.Identifier)
			atomic.AddInt32(&notificationCounter, 0x1)
		case <-ctx.Done():
			return
		}
	}()

	q.Push(willBeRemoved)
	q.Push(theOriginal)
	q.Push(shouldNotNotify)

	shouldNotNotify.State = types.S3
	q.Push(shouldNotNotify)

	q.Remove(willBeRemoved.Identifier)

	theOriginal.State = types.S3
	q.Push(theOriginal)

	time.Sleep(150 * time.Millisecond)
	notifications := atomic.LoadInt32(&notificationCounter)
	if notifications != 0x1 {
		t.Errorf("expected %d notifications, found %d", 1, notifications)
	}

	cancel()
	group.Wait()
}
