package fuzzy

import (
	"github.com/jabolina/go-mcast/test"
	"log"
	"testing"
	"time"
)

// This test will emit a command a time, iterating over
// a string slice containing the alphabet.
// This is only to verify if after a sequence of commands
// all partitions will end at the same state, since in this
// test no failure is injected over the transport.
func Test_SequentialCommands(t *testing.T) {
	cluster := test.CreateCluster(3, "alphabet", t)
	defer func() {
		ch := make(chan bool)
		defer close(ch)
		go func() {
			cluster.Off()
			ch <- true
		}()
		select {
		case <-ch:
			return
		case <-time.After(30 * time.Second):
			t.Error("failed shutdown cluster")
			return
		}
	}()

	key := []byte("alphabet")
	for _, letter := range test.Alphabet {
		log.Printf("************************** sending %s **************************", letter)
		req := test.GenerateRequest(key, []byte(letter), cluster.Names)
		obs := cluster.Next().Write(req)
		select {
		case res := <-obs:
			if !res.Success {
				t.Errorf("failed writting request %v", res.Failure)
				break
			}
		case <-time.After(500 * time.Millisecond):
			t.Errorf("write %s timeout %#v", letter, req)
			break
		}
	}

	time.Sleep(time.Second)
	cluster.DoesClusterMatchTo(key, []byte("Z"))
}
