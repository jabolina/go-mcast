package amcast

import (
	"fmt"
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

	add := func(port int) {
		defer wg.Done()
		address := mcast.ServerAddress(fmt.Sprintf("127.0.0.1:%d", port))
		destinations := []mcast.ServerAddress{
			address,
		}
		set.Add(destinations, mcast.UID(mcast.GenerateUID()))
	}

	for i := 0; i < concurrency; i++ {
		go add(i)
	}

	wg.Wait()

	if len(set.Values()) != concurrency {
		t.Fatalf("wanted %d elements on set, found %d", concurrency, len(set.Values()))
	}
}

func TestPreviousSet_ShouldConflict(t *testing.T) {
	set := amcast.NewPreviousSet()
	uid := mcast.UID(mcast.GenerateUID())
	address1 := mcast.ServerAddress("127.0.0.1:8080")
	address2 := mcast.ServerAddress("127.0.0.1:8081")
	destinations := []mcast.ServerAddress{
		address1,
		address2,
	}

	set.Add(destinations, uid)

	if !set.Conflicts(destinations) {
		t.Fatalf("the given uid should conflicts %s", uid)
	}
}

func TestPreviousSet_ShouldNotConflict(t *testing.T) {
	set := amcast.NewPreviousSet()
	address1 := mcast.ServerAddress("127.0.0.1:8080")
	address2 := mcast.ServerAddress("127.0.0.1:8081")
	destinations1 := []mcast.ServerAddress{
		address1,
	}
	destinations2 := []mcast.ServerAddress{
		address2,
	}

	set.Add(destinations1, mcast.UID(mcast.GenerateUID()))

	if set.Conflicts(destinations2) {
		t.Fatalf("the given uid should conflicts %v", set.Values())
	}
}
