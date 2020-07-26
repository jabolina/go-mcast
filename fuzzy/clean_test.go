package fuzzy

import (
	"github.com/jabolina/go-mcast/internal"
	"github.com/jabolina/go-mcast/test"
	"testing"
	"time"
)

// This will create a clean test, where multiple
// partitions with random generated names will start
// and with no failure injection after a second the
// partitions will be closed one by one.
func Test_CleanMultiplePartitions(t *testing.T) {
	p1 := internal.Partition(internal.GenerateUID())
	p2 := internal.Partition(internal.GenerateUID())
	p3 := internal.Partition(internal.GenerateUID())

	u1 := test.CreateUnity(p1, t)
	u2 := test.CreateUnity(p2, t)
	u3 := test.CreateUnity(p3, t)

	time.Sleep(time.Second)
	u1.Shutdown()

	time.Sleep(time.Second)
	u2.Shutdown()

	time.Sleep(time.Second)
	u3.Shutdown()
}
