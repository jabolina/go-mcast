package fuzzy

import (
	"context"
	"github.com/jabolina/go-mcast/test"
	"go.uber.org/goleak"
	"log"
	"sync"
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
		if !test.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			test.PrintStackTrace(t)
		}
		goleak.VerifyNone(t)
	}()

	for _, letter := range test.Alphabet {
		ctx, cancel := context.WithTimeout(context.TODO(), time.Second)
		log.Printf("************************** sending %s **************************", letter)
		req := test.GenerateRequest([]byte(letter), cluster.Names)
		select {
		case <-ctx.Done():
			t.Errorf("failed writing")
		case res := <-cluster.Next().Write(req):
			if !res.Success {
				t.Errorf("failed writting request %v", res.Failure)
				break
			}
		}
		cancel()
	}

	time.Sleep(10 * time.Second)
	cluster.DoesAllClusterMatch()
}

func Test_ConcurrentCommands(t *testing.T) {
	cluster := test.CreateCluster(3, "concurrent", t)
	defer func() {
		if !test.WaitThisOrTimeout(cluster.Off, 30*time.Second) {
			t.Error("failed shutdown cluster")
			test.PrintStackTrace(t)
		}
		goleak.VerifyNone(t)
	}()

	group := sync.WaitGroup{}
	write := func(idx int, val string) {
		defer group.Done()
		log.Printf("************************** sending %s **************************", val)
		req := test.GenerateRequest([]byte(val), cluster.Names)
		res := <-cluster.Next().Write(req)
		if !res.Success {
			t.Errorf("failed writting request %v", res.Failure)
		}
		t.Logf("finished %#v", res)
	}

	sample := test.Alphabet
	group.Add(len(sample))
	for i, content := range sample {
		go write(i, content)
	}

	if !test.WaitThisOrTimeout(group.Wait, 30*time.Second) {
		t.Errorf("not finished all after 30 seconds!")
		cluster.DoesAllClusterMatch()
	} else {
		time.Sleep(10 * time.Second)
		cluster.DoesAllClusterMatch()
	}
}
