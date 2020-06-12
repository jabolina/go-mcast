package test

import (
	"go-mcast/pkg/mcast"
	"testing"
)

func TestProtocol_BootstrapUnity(t *testing.T) {
	PoweroffUnity(CreateUnity(5, t), t)
}

func TestProtocol_BootstrapUnityCluster(t *testing.T) {
	cluster := CreateCluster(2, 3, t)
	cluster.Off()
}

// When dispatching a message to single unity containing a single
// process, the message will be transferred directly to state S3
// and can be delivered/committed.
// Since only exists a single node, the sequence number will not conflicts
// thus the clock will not be ticked.
func TestProtocol_GMCastMessageSingleUnitySingleProcess(t *testing.T) {
	unity := CreateUnity(1, t)
	defer PoweroffUnity(unity, t)

	peer := unity.ResolvePeer()
	req := mcast.GMCastRequest{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		UID: mcast.UID(mcast.GenerateUID()),
		Body: mcast.Message{
			MessageState: mcast.S0,
			Timestamp:    0,
			Data:         []byte("test"),
		},
		Destination: []mcast.Server{
			{
				ID:      peer.Id,
				Address: peer.Address,
			},
		},
	}

	var res mcast.GMCastResponse
	if err := peer.Trans.GMCast(peer.Id, peer.Address, &req, &res); err != nil {
		t.Fatalf("failed gmcast request %#v with %v", req, err)
	}

	if !res.Success {
		t.Fatalf("request failed computation. %#v", res)
	}

	if res.SequenceNumber != 0x0 {
		t.Fatalf("sequence number sould be 0, found %d", res.SequenceNumber)
	}
}

// This will create a single unity that holds 5 peers. Since the destination
// is only a single unity there will be no cross groups requests and the request
// state will jump directly to state S3.
// Since the message will be processed on multiple peers at the same time,
// the timestamp will be greater than 0.
func TestProtocol_GMCastMessageSingleUnityMultipleProcesses(t *testing.T) {
	unity := CreateUnity(5, t)
	defer PoweroffUnity(unity, t)

	peer := unity.ResolvePeer()
	req := mcast.GMCastRequest{
		RPCHeader: mcast.RPCHeader{
			ProtocolVersion: mcast.LatestProtocolVersion,
		},
		UID: mcast.UID(mcast.GenerateUID()),
		Body: mcast.Message{
			MessageState: mcast.S0,
			Timestamp:    0,
			Data:         []byte("test"),
		},
		Destination: []mcast.Server{
			{
				ID:      peer.Id,
				Address: peer.Address,
			},
		},
	}

	var res mcast.GMCastResponse
	if err := peer.Trans.GMCast(peer.Id, peer.Address, &req, &res); err != nil {
		t.Fatalf("failed gmcast request %#v with %v", req, err)
	}

	if !res.Success {
		t.Fatalf("request failed computation. %#v", res)
	}

	if res.SequenceNumber == 0x0 {
		t.Fatalf("sequence number should not be zero. %d", res.SequenceNumber)
	}
}
