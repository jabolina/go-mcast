package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/output"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
	"testing"
)

type logMetadata struct {
	mutex *sync.Mutex
	uids  []types.UID
}

func (l *logMetadata) add(uid types.UID) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.uids = append(l.uids, uid)
}

func (l *logMetadata) size() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.uids)
}

func TestLog_AppendAndRead(t *testing.T) {
	log := output.NewLogStructure(output.NewDefaultStorage())
	var uids []types.UID
	testSize := 1000
	for i := 0; i < testSize; i++ {
		uid := types.UID(helper.GenerateUID())
		msg := types.Message{
			Timestamp:  uint64(i),
			Identifier: uid,
			Content: types.DataHolder{
				Operation: types.Command,
			},
		}
		if err := log.Append(msg, false); err != nil {
			t.Errorf("failed appending %#v. %#v", msg, err)
		}
		uids = append(uids, uid)
	}

	if log.Size() != uint64(testSize) {
		t.Errorf("Expected 10 operations found %d", log.Size())
	}

	messages, err := log.Dump()
	if err != nil {
		t.Errorf("Failed reading message. %v", err)
	}

	if len(messages) != len(uids) {
		t.Errorf("expected %d messages, found %d", len(uids), len(messages))
	}

	for i, message := range messages {
		if message.Identifier != uids[i] {
			t.Errorf("expected UIDS %s, found %s", uids[i], message.Identifier)
		}
	}
}

func TestLog_ShouldHandleConcurrentOperations(t *testing.T) {
	log := output.NewLogStructure(output.NewDefaultStorage())
	uids := logMetadata{
		mutex: &sync.Mutex{},
	}
	testSize := 1000
	group := &sync.WaitGroup{}

	group.Add(testSize)
	for i := 0; i < testSize; i++ {
		appendMessage := func(ts int) {
			defer group.Done()
			uid := types.UID(helper.GenerateUID())
			msg := types.Message{
				Timestamp:  uint64(ts),
				Identifier: uid,
				Content: types.DataHolder{
					Operation: types.Command,
				},
			}
			if err := log.Append(msg, false); err != nil {
				t.Errorf("failed appending %#v. %#v", msg, err)
			}
			uids.add(uid)
		}
		go appendMessage(i)
	}

	group.Wait()

	if log.Size() != uint64(testSize) {
		t.Errorf("Expected 10 operations found %d", log.Size())
	}

	messages, err := log.Dump()
	if err != nil {
		t.Errorf("Failed reading message. %v", err)
	}

	if len(messages) != uids.size() {
		t.Errorf("expected %d messages, found %d", uids.size(), len(messages))
	}
}
