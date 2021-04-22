package fuzzy

import (
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test/util"
	"go.uber.org/goleak"
	"log"
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
	cluster := util.CreateClusterConflict("generic-sync", &ConflictWhenNeeded{}, ports, t)
	testSize := 50
	defer func() {
		if !util.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			util.PrintStackTrace(t)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	listener := func() {
		defer wg.Done()
		responseChan := cluster.Next().Listen()
		for {
			select {
			case res := <-responseChan:
				if !res.Success {
					t.Errorf("response with error %s. %#v", res.Failure.Error(), res.Data)
				}
			case <-time.After(5 * time.Second):
				t.Log("stop listening unity.")
				return
			}
		}
	}

	go listener()
	for i := 0; i < testSize; i++ {
		content := fmt.Sprintf("msg-%d", i)
		data := []byte(content)
		log.Printf("************************** sending [%s] **************************", content)
		req := util.GenerateRequest(data, cluster.Names)
		if i&0x1 == 0 {
			req.Extra = []byte(fmt.Sprintf("generic-%d", i))
		}
		if err := cluster.Next().Write(req); err != nil {
			t.Errorf("failed writting request %v", err)
		}
	}

	wg.Wait()
	cluster.DoesAllClusterMatch()
}

func Test_ShouldMaintainConsistencyWhenAsyncGeneric(t *testing.T) {
	defer goleak.VerifyNone(t)
	partition1 := []int{23000, 23001, 23002}
	partition2 := []int{23003, 23004, 23005}
	partition3 := []int{23006, 23007, 23008}
	ports := [][]int{partition1, partition2, partition3}
	cluster := util.CreateClusterConflict("generic-async", &ConflictWhenNeeded{}, ports, t)
	testSize := 50
	defer func() {
		if !util.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			util.PrintStackTrace(t)
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	listener := func() {
		defer wg.Done()
		responseChan := cluster.Next().Listen()
		for {
			select {
			case res := <-responseChan:
				if !res.Success {
					t.Errorf("response with error %s. %#v", res.Failure.Error(), res.Data)
				}
			case <-time.After(util.DefaultTestTimeout):
				t.Log("stop listening unity.")
				return
			}
		}
	}
	write := func(idx int, val string) {
		defer wg.Done()
		log.Printf("************************** sending [%s] **************************", val)
		req := util.GenerateRequest([]byte(val), cluster.Names)
		if idx&0x1 == 0 {
			req.Extra = []byte(fmt.Sprintf("generic-%d", idx))
		}
		if err := cluster.Next().Write(req); err != nil {
			t.Errorf("failed writting request %v", err)
		}
	}

	go listener()
	wg.Add(testSize)
	for i := 0; i < testSize; i++ {
		go write(i, fmt.Sprintf("msg-%d", i))
	}

	if !util.WaitThisOrTimeout(wg.Wait, 30*time.Second) {
		t.Errorf("not finished all after 30 seconds!")
	}
	cluster.DoesAllClusterMatch()
}
