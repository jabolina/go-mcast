package fuzzy

import (
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

	partition1 := []int{25000, 25001, 25002}
	partition2 := []int{25003, 25004, 25005}
	partition3 := []int{25006, 25007, 25008}

	partitionA := test.ProperPartitionName("mcast-sync-a", partition1)
	partitionB := test.ProperPartitionName("mcast-sync-b", partition2)
	partitionC := test.ProperPartitionName("mcast-sync-c", partition3)

	first := test.CreateUnity(partitionA, partition1, t)
	second := test.CreateUnity(partitionB, partition2, t)
	third := test.CreateUnity(partitionC, partition3, t)

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

	test.DoWeMatch(truth.Data, []test.Unity{second, third}, t)
}

// Will send commands to different partitions concurrently.
// After sending commands concurrently, commands should be accepted in
// the same order.
func Test_MulticastMessagesConcurrently(t *testing.T) {
	defer goleak.VerifyNone(t)

	partition1 := []int{26000, 26001, 26002}
	partition2 := []int{26003, 26004, 26005}
	partition3 := []int{26006, 26007, 26008}

	partitionA := test.ProperPartitionName("mcast-async-a", partition1)
	partitionB := test.ProperPartitionName("mcast-async-b", partition2)
	partitionC := test.ProperPartitionName("mcast-async-c", partition3)

	first := test.CreateUnity(partitionA, partition1, t)
	second := test.CreateUnity(partitionB, partition2, t)
	third := test.CreateUnity(partitionC, partition3, t)

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

	test.DoWeMatch(truth.Data, []test.Unity{second, third}, t)
}
