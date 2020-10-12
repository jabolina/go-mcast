package fuzzy

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test"
	"testing"
	"time"
)

// This will create a clean test, where multiple
// partitions with random generated names will start
// and with no failure injection after a second the
// partitions will be closed one by one.
func Test_CleanMultiplePartitions(t *testing.T) {
	p1 := types.Partition(helper.GenerateUID())
	p2 := types.Partition(helper.GenerateUID())
	p3 := types.Partition(helper.GenerateUID())

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
