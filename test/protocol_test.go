package test

import (
	"go-mcast/pkg/mcast"
	"testing"
)

func TestProtocol_BootstrapUnity(t *testing.T) {
	unity := CreateUnity(3, t)
	unity.Shutdown()
}

func TestProtocol_BootstrapUnityCluster(t *testing.T) {
	cluster := CreateCluster(3, 5, t)
	cluster.Off()
}

// When dispatching a message to single unity containing a single
// process, the message will be transferred directly to state S3
// and can be delivered/committed.
// Since only exists a single node, the sequence number will not conflicts
// thus the clock will not be ticked.
//
// Then a response will be queried back from the unity state machine.
func TestProtocol_GMCastMessageSingleUnitySingleProcess(t *testing.T) {
	unity := CreateUnity(1, t)
	defer unity.Shutdown()

	peer := unity.ResolvePeer()
	key := "test-key"
	value := "test"
	write := mcast.GMCastRequest{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		UID: mcast.UID(mcast.GenerateUID()),
		Body: mcast.Message{
			Data: mcast.DataHolder{
				Operation: mcast.Command,
				Key:       key,
				Content:   []byte(value),
			},
		},
		Destination: []mcast.Server{
			{
				ID:      peer.Id,
				Address: peer.Address,
			},
		},
	}

	var res mcast.GMCastResponse
	if err := peer.Trans.GMCast(peer.Id, peer.Address, &write, &res); err != nil {
		t.Fatalf("failed gmcast request %#v with %v", write, err)
	}

	if !res.Success {
		t.Fatalf("request failed computation. %#v", res)
	}

	if string(res.Body.Data.Content) != value {
		t.Fatalf("write response is different, expected [%s] found %s", value, string(res.Body.Data.Content))
	}

	if res.SequenceNumber != 0x0 {
		t.Fatalf("sequence number sould be 0, found %d", res.SequenceNumber)
	}

	// Now that the write request succeeded the value will
	// be queried back for validation.
	read := mcast.GMCastRequest{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		UID: mcast.UID(mcast.GenerateUID()),
		Body: mcast.Message{
			Data: mcast.DataHolder{
				Operation: mcast.Query,
				Key:       key,
			},
		},
		Destination: []mcast.Server{
			{
				ID:      peer.Id,
				Address: peer.Address,
			},
		},
	}

	var retrieved mcast.GMCastResponse
	if err := peer.Trans.GMCast(peer.Id, peer.Address, &read, &retrieved); err != nil {
		t.Fatalf("failed read request %#v with %v", write, err)
	}

	if !retrieved.Success {
		t.Fatalf("read failed computation. %#v", retrieved)
	}

	if string(retrieved.Body.Data.Content) != value {
		t.Fatalf("retrieved value was not %s found %s", value, string(retrieved.Body.Data.Content))
	}
}

// This will create a single unity that holds 5 peers. Since the destination
// is only a single unity there will be no cross groups requests and the request
// state will jump directly to state S3.
// Since the message will be processed on multiple peers at the same time,
// the timestamp will be greater than 0.
// After the write request, will be made read request into the unity state machine.
func TestProtocol_GMCastMessageSingleUnityMultipleProcesses(t *testing.T) {
	unity := CreateUnity(5, t)
	defer unity.Shutdown()

	peer := unity.ResolvePeer()
	key := "test-key-2"
	value := "test"
	write := mcast.GMCastRequest{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		UID: mcast.UID(mcast.GenerateUID()),
		Body: mcast.Message{
			Data: mcast.DataHolder{
				Operation: mcast.Command,
				Key:       key,
				Content:   []byte(value),
			},
		},
		Destination: []mcast.Server{
			{
				ID:      peer.Id,
				Address: peer.Address,
			},
		},
	}

	var writeRes mcast.GMCastResponse
	if err := peer.Trans.GMCast(peer.Id, peer.Address, &write, &writeRes); err != nil {
		t.Fatalf("failed gmcast request %#v with %v", write, err)
	}

	if !writeRes.Success {
		t.Fatalf("request failed computation. %#v", writeRes)
	}

	if writeRes.SequenceNumber == 0x0 {
		t.Fatalf("sequence number should not be zero. %d", writeRes.SequenceNumber)
	}

	if string(writeRes.Body.Data.Content) != value {
		t.Fatalf("written value different. expected %s found %s", value, string(writeRes.Body.Data.Content))
	}

	// Now query the state machine for the value back.
	read := mcast.GMCastRequest{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		UID: mcast.UID(mcast.GenerateUID()),
		Body: mcast.Message{
			Data: mcast.DataHolder{
				Operation: mcast.Query,
				Key:       key,
			},
		},
		Destination: []mcast.Server{
			{
				ID:      peer.Id,
				Address: peer.Address,
			},
		},
	}

	var retrieved mcast.GMCastResponse
	if err := peer.Trans.GMCast(peer.Id, peer.Address, &read, &retrieved); err != nil {
		t.Fatalf("failed read request %#v with %v", write, err)
	}

	if !retrieved.Success {
		t.Fatalf("read failed computation. %#v", retrieved)
	}

	if string(retrieved.Body.Data.Content) != value {
		t.Fatalf("retrieved value was not %s found %s", value, string(retrieved.Body.Data.Content))
	}
}
