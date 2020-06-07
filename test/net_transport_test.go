package test

import (
	"go-mcast/internal/remote"
	"go-mcast/pkg/mcast"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

type testAddressResolver struct {
	addr string
}

func (t *testAddressResolver) Resolve(id mcast.ServerID) (mcast.ServerAddress, error) {
	return mcast.ServerAddress(t.addr), nil
}

func makeTransport(useAddrProvider bool, address string) (*mcast.NetworkTransport, error) {
	if useAddrProvider {
		config := &mcast.NetworkTransportConfig{
			MaxPool:               2,
			Timeout:               time.Second,
			ServerAddressResolver: &testAddressResolver{addr: address},
		}
		return mcast.NewTCPTransportWithConfig("127.0.0.1:0", nil, config)
	}
	return mcast.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, os.Stdout)
}

// Create a new TCP network transport and closes the connection
func TestNetworkTransport_StartStop(t *testing.T) {
	trans, err := mcast.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, os.Stdout)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	trans.Close()
}

func TestNetworkTransport_PooledConn(t *testing.T) {
	consumer, err := makeTransport(true, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer consumer.Close()
	rpcCh := consumer.Consumer()

	args := mcast.GMCastRequest{
		RPCHeader: mcast.RPCHeader{ProtocolVersion: 0},
		UID:       "test-unique",
		Body: remote.Message{
			MessageState: 0,
			Timestamp:    0,
			Data:         []byte("hello, test!"),
			Extensions:   nil,
		},
	}
	resp := mcast.GMCastResponse{
		RPCHeader:      mcast.RPCHeader{ProtocolVersion: 0},
		SequenceNumber: 0,
		Success:        true,
	}

	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*mcast.GMCastRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Fatalf("command mismatch: %#v %#v", *req, args)
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	producer, err := makeTransport(false, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer producer.Close()

	// Create wait group
	wg := &sync.WaitGroup{}
	wg.Add(5)

	gmcast := func() {
		defer wg.Done()
		var out mcast.GMCastResponse
		if err := producer.GMCast("id1", consumer.LocalAddress(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}
	}

	for i := 0; i < 5; i++ {
		go gmcast()
	}

	// Wait for the routines to finish
	wg.Wait()
}

func TestNetworkTransport_GMCastRequest(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		consumer, err := makeTransport(useAddrProvider, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("could not create transport. %v", err)
		}
		defer consumer.Close()
		rpcCh := consumer.Consumer()

		req := mcast.GMCastRequest{
			RPCHeader: mcast.RPCHeader{ProtocolVersion: 0},
			UID:       "test-unique",
			Body: remote.Message{
				Data: []byte("hello, test!"),
			},
		}
		res := mcast.GMCastResponse{
			RPCHeader: mcast.RPCHeader{ProtocolVersion: 0},
			Success:   true,
		}

		go func() {
			select {
			case rpc := <-rpcCh:
				recv := rpc.Command.(*mcast.GMCastRequest)
				if !reflect.DeepEqual(recv, &req) {
					t.Fatalf("received wrong request. %#v %#v", *recv, req)
				}

				rpc.Respond(&res, nil)
			case <-time.After(250 * time.Millisecond):
				t.Fatalf("recv timeout")
			}
		}()

		producer, err := makeTransport(useAddrProvider, string(consumer.LocalAddress()))
		if err != nil {
			t.Fatalf("could not create producer. %v", err)
		}
		defer producer.Close()

		var recv mcast.GMCastResponse
		if err := producer.GMCast("id1", consumer.LocalAddress(), &req, &recv); err != nil {
			t.Fatalf("error receiving on producer. %v", err)
		}

		if !reflect.DeepEqual(recv, res) {
			t.Fatalf("received wrong response. %#v %#v", recv, res)
		}
	}
}

func TestNetworkTransport_ComputeRequest(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		consumer, err := makeTransport(useAddrProvider, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("could not create transport. %v", err)
		}
		defer consumer.Close()
		rpcCh := consumer.Consumer()

		req := mcast.ComputeRequest{
			RPCHeader: mcast.RPCHeader{ProtocolVersion: 0},
			UID:       "test-unique",
		}
		res := mcast.ComputeResponse{
			RPCHeader: mcast.RPCHeader{ProtocolVersion: 0},
			UID:       "test-unique",
		}

		go func() {
			select {
			case rpc := <-rpcCh:
				recv := rpc.Command.(*mcast.ComputeRequest)
				if !reflect.DeepEqual(recv, &req) {
					t.Fatalf("received wrong request. %#v %#v", *recv, req)
				}

				rpc.Respond(&res, nil)
			case <-time.After(250 * time.Millisecond):
				t.Fatalf("recv timeout")
			}
		}()

		producer, err := makeTransport(useAddrProvider, string(consumer.LocalAddress()))
		if err != nil {
			t.Fatalf("could not create producer. %v", err)
		}
		defer producer.Close()

		var recv mcast.ComputeResponse
		if err := producer.Compute("id1", consumer.LocalAddress(), &req, &recv); err != nil {
			t.Fatalf("error receiving on producer. %v", err)
		}

		if !reflect.DeepEqual(recv, res) {
			t.Fatalf("received wrong response. %#v %#v", recv, res)
		}
	}
}

func TestNetworkTransport_GatherRequest(t *testing.T) {
	for _, useAddrProvider := range []bool{true, false} {
		consumer, err := makeTransport(useAddrProvider, "127.0.0.1:0")
		if err != nil {
			t.Fatalf("could not create transport. %v", err)
		}
		defer consumer.Close()
		rpcCh := consumer.Consumer()

		req := mcast.GatherRequest{
			RPCHeader: mcast.RPCHeader{ProtocolVersion: 0},
			UID:       "test-unique",
		}
		res := mcast.GatherResponse{
			RPCHeader: mcast.RPCHeader{ProtocolVersion: 0},
			UID:       "test-unique",
		}

		go func() {
			select {
			case rpc := <-rpcCh:
				recv := rpc.Command.(*mcast.GatherRequest)
				if !reflect.DeepEqual(recv, &req) {
					t.Fatalf("received wrong request. %#v %#v", *recv, req)
				}

				rpc.Respond(&res, nil)
			case <-time.After(250 * time.Millisecond):
				t.Fatalf("recv timeout")
			}
		}()

		producer, err := makeTransport(useAddrProvider, string(consumer.LocalAddress()))
		if err != nil {
			t.Fatalf("could not create producer. %v", err)
		}
		defer producer.Close()

		var recv mcast.GatherResponse
		if err := producer.Gather("id1", consumer.LocalAddress(), &req, &recv); err != nil {
			t.Fatalf("error receiving on producer. %v", err)
		}

		if !reflect.DeepEqual(recv, res) {
			t.Fatalf("received wrong response. %#v %#v", recv, res)
		}
	}
}
