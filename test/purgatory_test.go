package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/hpq"
	"go.uber.org/goleak"
	"sync"
	"testing"
)

// Will concurrently add values to the cache structure.
// Then will verify that all added values are present and if
// they are added again, will return `false`.
func Test_ShouldConcurrentlySet(t *testing.T) {
	defer goleak.VerifyNone(t)
	wg := &sync.WaitGroup{}
	testSize := 50
	var ids []string

	c := hpq.NewTtlCache()

	insert := func(id string) {
		defer wg.Done()
		if !c.Set(id) {
			t.Errorf("failed setting %s", id)
		}
	}

	wg.Add(testSize)
	for i := 0; i < testSize; i++ {
		id := helper.GenerateUID()
		ids = append(ids, id)
		go insert(id)
	}

	wg.Wait()

	for _, id := range ids {
		if !c.Contains(id) {
			t.Errorf("should contains %s", id)
		}

		if c.Set(id) {
			t.Errorf("value was added late. %s", id)
		}
	}
}
