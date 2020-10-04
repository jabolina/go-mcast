package test

import (
	"bytes"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"testing"
	"time"
)

func TestProtocol_BootstrapUnity(t *testing.T) {
	partitionName := types.Partition("bootstrap-1-unity")
	unity := CreateUnity(partitionName, t)
	unity.Shutdown()
}

func TestProtocol_BootstrapUnityCluster(t *testing.T) {
	cluster := CreateCluster(3, "cluster", t)
	cluster.Off()
}

// When dispatching a message to single unity containing a single
// process, the message will be transferred directly to state S3
// and can be delivered/committed.
//
// Then a response will be queried back from the unity state machine.
func TestProtocol_GMCastMessageSingleUnitySingleProcess(t *testing.T) {
	partitionName := types.Partition("single.unity")
	unity := CreateUnity(partitionName, t)
	defer unity.Shutdown()
	key := []byte("test-key")
	value := []byte("test")
	write := types.Request{
		Key:         key,
		Value:       value,
		Destination: []types.Partition{partitionName},
	}

	obs := unity.Write(write)
	select {
	case res := <-obs:
		if !res.Success {
			t.Fatalf("failed writting request %v", res.Failure)
			return
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("write timeout")
		return
	}

	time.Sleep(time.Second)

	// Now that the write request succeeded the value will
	// be queried back for validation.
	read := types.Request{
		Key:         key,
		Destination: []types.Partition{partitionName},
	}

	res, err := unity.Read(read)
	if err != nil {
		t.Errorf("failed reading value %#v. %v", read, err)
	}

	if !res.Success {
		t.Errorf("read operation failed. %v", res.Failure)
	}

	if !bytes.Equal(value, res.Data) {
		t.Errorf("retrieved response should be %s but was %s", string(value), string(res.Data))
	}
}

// This will start two distinct partitions, a write command
// will be applied on both partitions. The command will be
// issued by one of the partitions.
// After the commit the current value will be queried back
// from another partition.
func TestProtocol_GMCastMessageTwoPartitions(t *testing.T) {
	partitionOne := types.Partition("single-unity-one")
	partitionTwo := types.Partition("single-unity-two")
	unityOne := CreateUnity(partitionOne, t)
	unityTwo := CreateUnity(partitionTwo, t)
	defer unityOne.Shutdown()
	key := []byte("test-key")
	value := []byte("test")
	write := types.Request{
		Key:         key,
		Value:       value,
		Destination: []types.Partition{partitionOne, partitionTwo},
	}

	obs := unityOne.Write(write)
	select {
	case res := <-obs:
		if !res.Success {
			t.Fatalf("failed writting request %v", res.Failure)
			return
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("write timeout")
		return
	}

	// See that with the observer above we know that the value
	// was committed in one of the peers inside one of the partitions
	// but we cannot guarantee that is already applied on all of the
	// partitions, this sleep is to avoid a sequential read the can
	// possibly fail.
	time.Sleep(time.Second)

	// Now that the write request succeeded the value will
	// be queried back for validation.
	read := types.Request{
		Key:         key,
		Destination: []types.Partition{partitionOne, partitionTwo},
	}

	res, err := unityTwo.Read(read)
	if err != nil {
		t.Errorf("failed reading value %#v. %v", read, err)
	}

	if !res.Success {
		t.Errorf("read operation failed. %v", res.Failure)
	}

	if !bytes.Equal(value, res.Data) {
		t.Errorf("retrieved response should be %s but was %s", string(value), string(res.Data))
	}
}

// This will start two distinct partitions, a write command
// will be applied only on a single partitions.
// After the write is applied correctly, it will be verified
// that the second partition do not contains the applied value
// while the first partition contains.
func TestProtocol_TwoPartitionsSingleParticipant(t *testing.T) {
	partitionOne := types.Partition("a-single-unity-one")
	partitionTwo := types.Partition("b-single-unity-two")
	unityOne := CreateUnity(partitionOne, t)
	unityTwo := CreateUnity(partitionTwo, t)
	defer func() {
		unityOne.Shutdown()
		unityTwo.Shutdown()
	}()

	key := []byte("test-key")
	value := []byte("test")
	write := types.Request{
		Key:         key,
		Value:       value,
		Destination: []types.Partition{partitionOne},
	}

	// First a value will be written on the state machine for
	// only the first partition.
	obs := unityOne.Write(write)
	select {
	case res := <-obs:
		if !res.Success {
			t.Fatalf("failed writting request %v", res.Failure)
			return
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("write timeout")
		return
	}

	// Now that the write request was applied to the first partition
	// we verify that the second partition do not contains the applied
	// value.
	doNotExist := types.Request{
		Key:         key,
		Destination: []types.Partition{partitionOne, partitionTwo},
	}

	res, err := unityTwo.Read(doNotExist)
	if err == nil {
		t.Errorf("read should have failed %v. %v", doNotExist, err)
	}

	if res.Success {
		t.Errorf("read operation succeded. %v", res.Failure)
	}

	if res.Data != nil && len(res.Data) > 0 {
		t.Errorf("read operation should not contain any value. %s", string(res.Data))
	}

	// Now that we verified that the second partition do not have the
	// applied command, the first partition is verified and it must
	// contains the applied request.
	read := types.Request{
		Key:         key,
		Destination: []types.Partition{partitionOne, partitionTwo},
	}

	res, err = unityOne.Read(read)
	if err != nil {
		t.Errorf("failed reading value %v. %v", read, err)
	}

	if !res.Success {
		t.Errorf("read operation failed. %v", res.Failure)
	}

	if !bytes.Equal(value, res.Data) {
		t.Errorf("retrieved response should be %s but was %s", string(value), string(res.Data))
	}
}
