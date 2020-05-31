package go_mcast

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

type testAddressResolver struct {
	addr string
}

func (t *testAddressResolver) Resolve(id ServerID) (ServerAddress, error) {
	return ServerAddress(t.addr), nil
}

// Create a new TCP network transport and closes the connection
func TestNetworkTransport_StartStop(t *testing.T) {
	trans, err := NewTCPTransportWithLogger("127.0.0.1:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	trans.Close()
}

func TestNetworkTransport_PooledConn(t *testing.T) {
	consumer, err := NewTCPTransportWithLogger("127.0.0.1:0", nil, 2, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer consumer.Close()
	rpcCh := consumer.Consumer()

	args := GMCastRequest{
		RPCHeader:   RPCHeader{ProtocolVersion: 0},
		UID:         "test-unique",
		Body:        Message{
			State:      0,
			Timestamp:  0,
			Data:       []byte("hello, test!"),
			Extensions: nil,
		},
	}
	resp := GMCastResponse{
		RPCHeader:      RPCHeader{ProtocolVersion: 0},
		SequenceNumber: 0,
		Success:        true,
	}

	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*GMCastRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Fatalf("command mismatch: %#v %#v", *req, args)
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	producer, err := NewTCPTransportWithLogger("127.0.0.1:0", nil, 3, time.Second, newTestLogger(t))
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer producer.Close()

	// Create wait group
	wg := &sync.WaitGroup{}
	wg.Add(5)

	appendFunc := func() {
		defer wg.Done()
		var out GMCastResponse
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

	// Check the conn pool size
	addr := consumer.LocalAddress()
	if len(producer.connPool[addr]) != 3 {
		t.Fatalf("Expected 3 pooled conns!")
	}
}
