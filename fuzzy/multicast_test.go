package fuzzy

import (
	"github.com/jabolina/go-mcast/pkg/mcast"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test"
	"go.uber.org/goleak"
	"sync"
	"testing"
	"time"
)

// Will try send commands sequentially to different partitions.
// This will verify if the commands are still delivered in the same
// order across different partitions.
func Test_MulticastSequentialCommands(t *testing.T) {
	defer goleak.VerifyNone(t)

	partitionA := types.Partition("partition-a")
	partitionB := types.Partition("partition-b")
	partitionC := types.Partition("partition-c")

	first := test.CreateUnity(partitionA, t)
	second := test.CreateUnity(partitionB, t)
	third := test.CreateUnity(partitionC, t)

	defer first.Shutdown()
	defer second.Shutdown()
	defer third.Shutdown()

	broadcast := test.GenerateRequest([]byte("broadcast"), []types.Partition{partitionA, partitionB, partitionC})
	res := <-first.Write(broadcast)
	if !res.Success {
		t.Errorf("failed broadcasting first message. %#v", res.Failure)
	}

	onlySomePartitions := test.GenerateRequest([]byte("secret"), []types.Partition{partitionB, partitionC})
	res = <-second.Write(onlySomePartitions)
	if !res.Success {
		t.Errorf("failed sending to some partitions. %#v", res.Failure)
	}

	time.Sleep(time.Second)
	truth := second.Read()
	if !truth.Success {
		t.Errorf("failed reading values. %#v", truth.Failure)
	}

	test.DoWeMatch(truth.Data, []mcast.Unity{second, third}, t)
}

// Will send commands to different partitions concurrently.
// After sending commands concurrently, commands should be accepted in
// the same order.
func Test_MulticastMessagesConcurrently(t *testing.T) {
	defer goleak.VerifyNone(t)

	partitionA := types.Partition("partition-a")
	partitionB := types.Partition("partition-b")
	partitionC := types.Partition("partition-c")

	first := test.CreateUnity(partitionA, t)
	second := test.CreateUnity(partitionB, t)
	third := test.CreateUnity(partitionC, t)

	wait := &sync.WaitGroup{}

	defer first.Shutdown()
	defer second.Shutdown()
	defer third.Shutdown()

	wait.Add(2)

	sendBroadcast := func() {
		defer wait.Done()
		broadcast := test.GenerateRequest([]byte("broadcast"), []types.Partition{partitionA, partitionB, partitionC})
		res := <-first.Write(broadcast)
		if !res.Success {
			t.Errorf("failed broadcasting first message. %#v", res.Failure)
		}
	}

	sendMulticast := func() {
		defer wait.Done()
		onlySomePartitions := test.GenerateRequest([]byte("secret"), []types.Partition{partitionB, partitionC})
		res := <-second.Write(onlySomePartitions)
		if !res.Success {
			t.Errorf("failed sending to some partitions. %#v", res.Failure)
		}
	}

	go sendBroadcast()
	go sendMulticast()

	wait.Wait()
	time.Sleep(time.Second)
	truth := second.Read()
	if !truth.Success {
		t.Errorf("failed reading values. %#v", truth.Failure)
	}

	test.DoWeMatch(truth.Data, []mcast.Unity{second, third}, t)
}
