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

// Create a new TCP network transport and closes the connection
func TestNetworkTransport_StartStop(t *testing.T) {
	trans, err := mcast.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, os.Stdout)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	trans.Close()
}

func TestNetworkTransport_PooledConn(t *testing.T) {
	consumer, err := mcast.NewTCPTransport("127.0.0.1:0", nil, 2, time.Second, os.Stdout)
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

	producer, err := mcast.NewTCPTransport("127.0.0.1:0", nil, 3, time.Second, os.Stdout)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer producer.Close()

	// Create wait group
	wg := &sync.WaitGroup{}
	wg.Add(5)

	appendFunc := func() {
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

	// Try to do parallel appends, should stress the conn pool
	for i := 0; i < 5; i++ {
		go appendFunc()
	}

	// Wait for the routines to finish
	wg.Wait()
}
