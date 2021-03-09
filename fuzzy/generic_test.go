package fuzzy

import (
	"context"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test"
	"go.uber.org/goleak"
	"sync"
	"testing"
	"time"
)

type ConflictWhenNeeded struct {}

func (c *ConflictWhenNeeded) Conflict(message types.Message, _ []types.Message) bool {
	return message.Content.Extensions == nil
}

func Test_ShouldMaintainConsistencyWhenSyncGeneric(t *testing.T) {
	defer goleak.VerifyNone(t)
	cluster := test.CreateClusterConflict(3, "generic-sync", &ConflictWhenNeeded{}, t)
	testSize := 50
	defer func() {
		if !test.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			test.PrintStackTrace(t)
		}
	}()

	for i := 0; i < testSize; i++ {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
		data := []byte(fmt.Sprintf("msg-%d", i))
		req := test.GenerateRequest(data, cluster.Names)
		if i&0x1 == 0 {
			req.Extra = []byte(fmt.Sprintf("generic-%d", i))
		}
		select {
		case <-ctx.Done():
			t.Errorf("failed writing")
		case res := <-cluster.Next().Write(req):
			if !res.Success {
				t.Errorf("failed writing request. %#v", res.Failure)
				break
			}
		}
		cancel()
	}

	time.Sleep(5 * time.Second)
	cluster.DoesAllClusterMatch()
}

func Test_ShouldMaintainConsistencyWhenAsyncGeneric(t *testing.T) {
	defer goleak.VerifyNone(t)
	cluster := test.CreateClusterConflict(3, "generic-async", &ConflictWhenNeeded{}, t)
	testSize := 50
	defer func() {
		if !test.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			test.PrintStackTrace(t)
		}
	}()

	writers := sync.WaitGroup{}
	write := func(idx int, val string) {
		defer writers.Done()
		req := test.GenerateRequest([]byte(val), cluster.Names)
		if idx&0x1 == 0 {
			req.Extra = []byte(fmt.Sprintf("generic-%d", idx))
		}
		res := <-cluster.Next().Write(req)
		if !res.Success {
			t.Errorf("failed writting request %v", res.Failure)
		}
	}

	writers.Add(testSize)
	for i := 0; i < testSize; i++ {
		go write(i, fmt.Sprintf("msg-%d", i))
	}

	if !test.WaitThisOrTimeout(writers.Wait, 30*time.Second) {
		t.Errorf("not finished all after 30 seconds!")
		cluster.DoesAllClusterMatch()
	} else {
		time.Sleep(10 * time.Second)
		cluster.DoesAllClusterMatch()
	}
}
