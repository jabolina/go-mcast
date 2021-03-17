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

type ConflictWhenNeeded struct{}

func (c *ConflictWhenNeeded) Conflict(message types.Message, _ []types.Message) bool {
	return message.Content.Extensions == nil
}

func Test_ShouldMaintainConsistencyWhenSyncGeneric(t *testing.T) {
	defer goleak.VerifyNone(t)
	partition1 := []int{21000, 21001, 21002}
	partition2 := []int{21003, 21004, 21005}
	partition3 := []int{21006, 21007, 21008}
	ports := [][]int{partition1, partition2, partition3}
	cluster := test.CreateClusterConflict("generic-sync", &ConflictWhenNeeded{}, ports, t)
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
			t.Error("failed writing")
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
	partition1 := []int{23000, 23001, 23002}
	partition2 := []int{23003, 23004, 23005}
	partition3 := []int{23006, 23007, 23008}
	ports := [][]int{partition1, partition2, partition3}
	cluster := test.CreateClusterConflict("generic-async", &ConflictWhenNeeded{}, ports, t)
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
	} else {
		time.Sleep(10 * time.Second)
	}
	cluster.DoesAllClusterMatch()
}
