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

// If a correct process GM-Cast a message `m` to `m.dest`, then
// some process in `m.dest` eventually GM-Deliver `m`.
func Test_ValidityProperty(t *testing.T) {
	defer goleak.VerifyNone(t)

	partition1 := []int{21200, 21201, 21202}
	partition2 := []int{21203, 21204, 21205}
	partition3 := []int{21206, 21207, 21208}

	ports := [][]int{partition1, partition2, partition3}

	cluster := util.CreateCluster("validity", ports, t)
	defer func() {
		if !util.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			util.PrintStackTrace(t)
		}
	}()

	gmDeliver := make(chan interface{})

	wg := sync.WaitGroup{}
	wg.Add(1)

	listener := func() {
		defer wg.Done()
		select {
		case res := <-cluster.Next().Listen():
			if !res.Success {
				t.Errorf("response with error %s. %#v", res.Failure.Error(), res.Data)
			}
			gmDeliver <- true
		case <-time.After(util.DefaultTestTimeout):
			t.Log("stop listening unity.")
		}
	}

	go listener()
	req := util.GenerateRequest([]byte("ehlo"), cluster.Names)
	if err := cluster.Next().Write(req); err != nil {
		t.Errorf("failed writting request %v", err)
	}

	select {
	case <-gmDeliver:
	case <-time.After(util.DefaultTestTimeout):
		t.Errorf("did not eventually delivered")
	}

	wg.Wait()
}

// Agreement states that:
// If a correct process GM-Deliver a message `m`, then all correct
// processes in `m.dest` eventually GM-Deliver `m`.
//
// And Integrity:
// For any message `m`, every correct process in `m.dest` GM-Deliver
// `m` at most once, and only if `m` was previously GM-Cast by some process.
func Test_AgreementAndIntegrityProperty(t *testing.T) {
	defer goleak.VerifyNone(t)

	partition1 := []int{16100, 16101, 16102}
	partition2 := []int{16103, 16104, 16105}
	partition3 := []int{16106, 16107, 16108}

	partitionA := util.ProperPartitionName("agreement-a", partition1)
	partitionB := util.ProperPartitionName("agreement-b", partition2)
	partitionC := util.ProperPartitionName("agreement-c", partition3)
	partitions := []types.Partition{partitionA, partitionB, partitionC}

	first := util.CreateUnity(partitionA, partition1, t)
	second := util.CreateUnity(partitionB, partition2, t)
	third := util.CreateUnity(partitionC, partition3, t)

	defer first.Shutdown()
	defer second.Shutdown()
	defer third.Shutdown()

	wg := &sync.WaitGroup{}

	wg.Add(1)
	sendBroadcast := func(unity util.Unity) {
		defer wg.Done()
		broadcast := util.GenerateRequest([]byte(helper.GenerateUID()), partitions)
		err := unity.Write(broadcast)
		if err != nil {
			t.Errorf("failed sending message to %v. %#v", partitions, err)
		}
	}

	go sendBroadcast(first)
	wg.Wait()

	time.Sleep(util.DefaultTestTimeout)
	util.CompareOutputs(t, []util.Unity{first, second, third}, func(data types.DataHolder) bool {
		return true
	})
}

// If a correct process `p` and `q` both GM-Deliver messages `m` and `n`,
// ({p, q} \in m.dest \inter n.dest) and Conflict(m, n), then `p` GM-Deliver
// `m` before `n` iff `q` GM-Deliver `m` before `n`.
func Test_PartialOrderProperty(t *testing.T) {
	defer goleak.VerifyNone(t)

	partition1 := []int{16100, 16101, 16102}
	partition2 := []int{16103, 16104, 16105}
	partition3 := []int{16106, 16107, 16108}

	partitionA := util.ProperPartitionName("partial-order-a", partition1)
	partitionB := util.ProperPartitionName("partial-order-b", partition2)
	partitionC := util.ProperPartitionName("partial-order-c", partition3)

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

	wg.Add(sampleSize)
	for i := 0; i < sampleSize; i++ {
		go sendBroadcast(first, nil, []types.Partition{partitionA, partitionB, partitionC})
	}

	wg.Wait()
	time.Sleep(util.DefaultTestTimeout)

	util.CompareOutputs(t, []util.Unity{first, second}, func(data types.DataHolder) bool {
		return data.Extensions == nil || bytes.Equal(data.Extensions, ab)
	})

	util.CompareOutputs(t, []util.Unity{second, third}, func(data types.DataHolder) bool {
		return data.Extensions == nil || bytes.Equal(data.Extensions, bc)
	})

	util.CompareOutputs(t, []util.Unity{first, third}, func(data types.DataHolder) bool {
		return data.Extensions == nil || bytes.Equal(data.Extensions, ac)
	})

	util.CompareOutputs(t, []util.Unity{first, second, third}, func(data types.DataHolder) bool {
		return data.Extensions == nil
	})
}
