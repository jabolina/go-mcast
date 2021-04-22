package fuzzy

import (
	"bytes"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test/util"
	"go.uber.org/goleak"
	"sync"
	"testing"
	"time"
)

// Will send commands to different partitions concurrently.
// After sending commands concurrently, commands should be accepted in
// the same order across partitions.
func Test_MulticastMessagesConcurrently(t *testing.T) {
	defer goleak.VerifyNone(t)

	partition1 := []int{26000, 26001, 26002}
	partition2 := []int{26003, 26004, 26005}
	partition3 := []int{26006, 26007, 26008}

	partitionA := util.ProperPartitionName("mcast-async-a", partition1)
	partitionB := util.ProperPartitionName("mcast-async-b", partition2)
	partitionC := util.ProperPartitionName("mcast-async-c", partition3)

	first := util.CreateUnity(partitionA, partition1, t)
	second := util.CreateUnity(partitionB, partition2, t)
	third := util.CreateUnity(partitionC, partition3, t)

	sampleSize := 10

	wg := &sync.WaitGroup{}

	defer first.Shutdown()
	defer second.Shutdown()
	defer third.Shutdown()

	ab := []byte("AB")
	bc := []byte("BC")
	ac := []byte("AC")

	sendBroadcast := func(unity util.Unity, extra []byte, partitions []types.Partition) {
		defer wg.Done()
		broadcast := util.GenerateRequest([]byte(helper.GenerateUID()), partitions)
		broadcast.Extra = extra
		err := unity.Write(broadcast)
		if err != nil {
			t.Errorf("failed sending message to %v. %#v", partitions, err)
		}
	}

	wg.Add(sampleSize)
	for i := 0; i < sampleSize; i++ {
		go sendBroadcast(first, ab, []types.Partition{partitionA, partitionB})
	}

	wg.Add(sampleSize)
	for i := 0; i < sampleSize; i++ {
		go sendBroadcast(second, bc, []types.Partition{partitionB, partitionC})
	}

	wg.Add(sampleSize)
	for i := 0; i < sampleSize; i++ {
		go sendBroadcast(third, ac, []types.Partition{partitionA, partitionC})
	}

	wg.Wait()
	time.Sleep(util.DefaultTestTimeout)

	util.CompareOutputs(t, []util.Unity{first, second}, func(data types.DataHolder) bool {
		return bytes.Equal(data.Extensions, ab)
	})

	util.CompareOutputs(t, []util.Unity{second, third}, func(data types.DataHolder) bool {
		return bytes.Equal(data.Extensions, bc)
	})

	util.CompareOutputs(t, []util.Unity{first, third}, func(data types.DataHolder) bool {
		return bytes.Equal(data.Extensions, ac)
	})
}
