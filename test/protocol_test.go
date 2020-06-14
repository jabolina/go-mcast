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

// This will create a cluster of unities and requests message using all groups
// as destinations.
// First will be made a write request to unity 1 and then will be made a
// read request to unity 2.
// The read value must be the same as the written value, since the value
// will be replicated across unities.
func TestProtocol_TestMultipleUnities(t *testing.T) {
	cluster := CreateCluster(2, 5, t)
	defer cluster.Off()
	peers := cluster.Peers()
	var destination []mcast.Server
	for _, peer := range peers {
		destination = append(destination, mcast.Server{
			ID:      peer.Id,
			Address: peer.Address,
		})
	}

	key := "test-key"
	value := "test"
	write := mcast.Request{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		Key:         []byte(key),
		Value:       []byte(value),
		Destination: destination,
		Operation:   mcast.Command,
	}

	peer1 := peers[0]
	var res mcast.Response
	if err := peer1.Trans.Request(peer1.Id, peer1.Address, &write, &res); err != nil {
		t.Fatalf("failed gmcast request %#v with %v", write, err)
	}

	if !res.Success {
		t.Fatalf("request failed computation. %#v", res)
	}

	if string(res.Value) != value {
		t.Fatalf("write response is different, expected [%s] found %s", value, string(res.Value))
	}

	// Now that the write request succeeded the value will
	// be queried back for validation from another unity.
	read := mcast.Request{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		Operation:   mcast.Query,
		Key:         []byte(key),
		Destination: destination,
	}

	peer2 := peers[0]
	var retrieved mcast.Response
	if err := peer2.Trans.Request(peer2.Id, peer2.Address, &read, &retrieved); err != nil {
		t.Fatalf("failed read request %#v with %v", write, err)
	}

	if !retrieved.Success {
		t.Fatalf("read failed computation. %#v", retrieved)
	}

	if string(retrieved.Value) != value {
		t.Fatalf("retrieved value was not %s found %s", value, string(retrieved.Value))
	}
}
