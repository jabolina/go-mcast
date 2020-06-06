package amcast

import (
	"go-mcast/internal/amcast"
	"go-mcast/pkg/mcast"
	"sync"
	"testing"
)

// Test for concurrent adds on the previous set
// this test will fail if the generated 128-bit ID have a collision.
func TestPreviousSet_ConcurrentAdd(t *testing.T) {
	concurrency := 50
	set := amcast.NewPreviousSet()

	wg := &sync.WaitGroup{}
	wg.Add(concurrency)

	add := func() {
		defer wg.Done()
		set.Add(mcast.UID(mcast.GenerateUID()))
	}

	for i := 0; i < concurrency; i++ {
		go add()
	}

	wg.Wait()

	if len(set.Values()) != concurrency {
		t.Fatalf("wanted %d elements on set, found %d", concurrency, len(set.Values()))
	}
}
