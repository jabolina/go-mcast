package fuzzy

import (
	"github.com/jabolina/go-mcast/test/util"
	"go.uber.org/goleak"
	"log"
	"sync"
	"testing"
	"time"
)

// This test will emit a command a time, iterating over
// a string slice containing the alphabet.
// This is only to verify if after a sequence of commands
// all partitions will end at the same state, since in this
// test no failure is injected over the transport.
func Test_BroadcastSequentialCommands(t *testing.T) {
	defer goleak.VerifyNone(t)
	partition1 := []int{24000, 24001, 24002}
	partition2 := []int{24003, 24004, 24005}
	partition3 := []int{24006, 24007, 24008}
	ports := [][]int{partition1, partition2, partition3}
	cluster := util.CreateCluster("alphabet", ports, t)
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

	go listener()
	for _, letter := range util.Alphabet {
		log.Printf("************************** sending %s **************************", letter)
		req := util.GenerateRequest([]byte(letter), cluster.Names)
		if err := cluster.Next().Write(req); err != nil {
			t.Errorf("failed writting request %v", err)
		}
	}

	wg.Wait()
	cluster.DoesAllClusterMatch()
}

func Test_BroadcastConcurrentCommands(t *testing.T) {
	defer goleak.VerifyNone(t)
	partition1 := []int{20000, 20001, 20002}
	partition2 := []int{20003, 20004, 20005}
	partition3 := []int{20006, 20007, 20008}
	ports := [][]int{partition1, partition2, partition3}
	cluster := util.CreateCluster("concurrent", ports, t)
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

	go listener()
	write := func(idx int, val string) {
		defer wg.Done()
		log.Printf("************************** sending %s **************************", val)
		req := util.GenerateRequest([]byte(val), cluster.Names)
		if err := cluster.Next().Write(req); err != nil {
			t.Errorf("failed writting request %v", err)
		}
	}

	sample := util.Alphabet
	wg.Add(len(sample))
	for i, content := range sample {
		go write(i, content)
	}

	if !util.WaitThisOrTimeout(wg.Wait, 30*time.Second) {
		t.Errorf("not finished all after 30 seconds!")
	}
	cluster.DoesAllClusterMatch()
}
