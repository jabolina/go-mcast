package test

import (
	"bytes"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test/util"
	"testing"
	"time"
)

// When dispatching a message to single unity containing a single
// process, the message will be transferred directly to state S3
// and can be delivered/committed.
//
// Then a response will be queried back from the unity state machine.
func TestProtocol_GMCastMessageSingleUnitySingleProcess(t *testing.T) {
	ports := []int{32200}
	partitionName := util.ProperPartitionName("single.unity", ports)
	unity := util.CreateUnity(partitionName, ports, t)
	defer unity.Shutdown()
	value := []byte("test")
	write := types.Request{
		Value:       value,
		Destination: []types.Partition{partitionName},
	}

	if err := unity.Write(write); err != nil {
		t.Fatalf("failed writting request %v", err)
	}

	time.Sleep(time.Second)

	// Now that the write request succeeded the value will
	// be queried back for validation.
	res := unity.Read()
	if !res.Success {
		t.Errorf("read operation failed. %v", res.Failure)
	}

	if len(res.Data) != 1 {
		t.Errorf("Expected 1 command, found %d", len(res.Data))
	}

	cmd := res.Data[0]
	if !bytes.Equal(value, cmd.Content) {
		t.Errorf("retrieved response should be %s but was %s", string(value), string(cmd.Content))
	}
}

// This will start two distinct partitions, a write command
// will be applied on both partitions. The command will be
// issued by one of the partitions.
// After the commit the current value will be queried back
// from another partition.
func TestProtocol_GMCastMessageTwoPartitions(t *testing.T) {
	portFirst := []int{32020}
	portSnd := []int{32030}
	partitionOne := util.ProperPartitionName("single-unity-one", portFirst)
	partitionTwo := util.ProperPartitionName("single-unity-two", portSnd)
	unityOne := util.CreateUnity(partitionOne, portFirst, t)
	unityTwo := util.CreateUnity(partitionTwo, portSnd, t)
	defer unityOne.Shutdown()
	defer unityTwo.Shutdown()
	value := []byte("test")
	write := types.Request{
		Value:       value,
		Destination: []types.Partition{partitionOne, partitionTwo},
	}

	if err := unityOne.Write(write); err != nil {
		t.Fatalf("failed writting request %v", err)
	}

	// See that with the observer above we know that the value
	// was committed in one of the peers inside one of the partitions
	// but we cannot guarantee that is already applied on all of the
	// partitions, this sleep is to avoid a sequential read the can
	// possibly fail.
	time.Sleep(time.Second)

	// Now that the write request succeeded the value will
	// be queried back for validation.
	res := unityTwo.Read()
	if !res.Success {
		t.Errorf("read operation failed. %v", res.Failure)
	}

	if len(res.Data) != 1 {
		t.Errorf("Expected 1 command, found %d", len(res.Data))
		return
	}

	cmd := res.Data[0]
	if !bytes.Equal(value, cmd.Content) {
		t.Errorf("retrieved response should be %s but was %s", string(value), string(cmd.Content))
	}
}

// This will start two distinct partitions, a write command
// will be applied only on a single partitions.
// After the write is applied correctly, it will be verified
// that the second partition do not contains the applied value
// while the first partition contains.
func TestProtocol_TwoPartitionsSingleParticipant(t *testing.T) {
	portFst := []int{32000}
	portSnd := []int{32010}
	partitionOne := util.ProperPartitionName("a-single-unity-one", portFst)
	partitionTwo := util.ProperPartitionName("b-single-unity-two", portSnd)
	unityOne := util.CreateUnity(partitionOne, portFst, t)
	unityTwo := util.CreateUnity(partitionTwo, portSnd, t)
	defer func() {
		unityOne.Shutdown()
		unityTwo.Shutdown()
	}()

	value := []byte("test")
	write := types.Request{
		Value:       value,
		Destination: []types.Partition{partitionOne},
	}

	// First a value will be written on the state machine for
	// only the first partition.
	if err := unityOne.Write(write); err != nil {
		t.Fatalf("failed writting request %v", err)
	}

	time.Sleep(time.Second)
	// Now that the write request was applied to the first partition
	// we verify that the second partition do not contains the applied
	// value.
	res := unityTwo.Read()

	if res.Failure != nil {
		t.Errorf("read operation succeded. %v", res.Failure)
	}

	if res.Data != nil && len(res.Data) > 0 {
		t.Errorf("read operation should not contain any value. Found %d", len(res.Data))
	}

	// Now that we verified that the second partition do not have the
	// applied command, the first partition is verified and it must
	// contains the applied request.
	res = unityOne.Read()
	if !res.Success {
		t.Errorf("read operation failed. %v", res.Failure)
	}

	if len(res.Data) != 1 {
		t.Errorf("Expected 1 command, found %d", len(res.Data))
	}

	cmd := res.Data[0]
	if !bytes.Equal(value, cmd.Content) {
		t.Errorf("retrieved response should be %s but was %s", string(value), string(cmd.Content))
	}
}
