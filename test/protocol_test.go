package test

import (
	"bytes"
	"github.com/jabolina/go-mcast/internal"
	"testing"
	"time"
)

func TestProtocol_BootstrapUnity(t *testing.T) {
	partitionName := internal.Partition("bootstrap-1-unity")
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
	partitionName := internal.Partition("single.unity")
	unity := CreateUnity(partitionName, t)
	defer unity.Shutdown()
	key := []byte("test-key")
	value := []byte("test")
	write := internal.Request{
		Key:         key,
		Value:       value,
		Destination: []internal.Partition{partitionName},
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

	// Now that the write request succeeded the value will
	// be queried back for validation.
	read := internal.Request{
		Key:         key,
		Destination: []internal.Partition{partitionName},
	}

	res, err := unity.Read(read)
	if err != nil {
		t.Fatalf("failed reading value %#v. %v", read, err)
	}

	if !res.Success {
		t.Fatalf("read operation failed. %v", res.Failure)
	}

	if !bytes.Equal(value, res.Data) {
		t.Fatalf("retrieved response should be %s but was %s", string(value), string(res.Data))
	}
}

// This will start two distinct partitions, a write command
// will be applied on both partitions. The command will be
// issued by one of the partitions.
// After the commit the current value will be queried back
// from another partition.
func TestProtocol_GMCastMessageTwoPartitions(t *testing.T) {
	partitionOne := internal.Partition("single-unity-one")
	partitionTwo := internal.Partition("single-unity-two")
	unityOne := CreateUnity(partitionOne, t)
	unityTwo := CreateUnity(partitionTwo, t)
	defer unityOne.Shutdown()
	key := []byte("test-key")
	value := []byte("test")
	write := internal.Request{
		Key:         key,
		Value:       value,
		Destination: []internal.Partition{partitionOne, partitionTwo},
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

	// Now that the write request succeeded the value will
	// be queried back for validation.
	read := internal.Request{
		Key:         key,
		Destination: []internal.Partition{partitionOne, partitionTwo},
	}

	res, err := unityTwo.Read(read)
	if err != nil {
		t.Fatalf("failed reading value %v. %v", read, err)
	}

	if !res.Success {
		t.Fatalf("read operation failed. %v", res.Failure)
	}

	if !bytes.Equal(value, res.Data) {
		t.Fatalf("retrieved response should be %s but was %s", string(value), string(res.Data))
	}
}
