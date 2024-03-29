package test

import (
	"context"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/definition"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/network"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/jabolina/go-mcast/test/util"
	"go.uber.org/goleak"
	"os"
	"sync"
	"testing"
	"time"
)

func Test_TransportActAsAUnity(t *testing.T) {
	defer goleak.VerifyNone(t)

	_, isCi := os.LookupEnv("CI_ENV")
	partition := "transport-unity-" + helper.GenerateUID()
	testSize := 100
	clusterSize := 30
	ctx, cancel := context.WithCancel(context.TODO())
	listenersGroup := &sync.WaitGroup{}
	writersGroup := &sync.WaitGroup{}
	initializeReplica := func(trans network.Transport, h *util.MessageHist) {
		listenChan := trans.Listen()
		go func() {
			defer listenersGroup.Done()
			for {
				select {
				case m := <-listenChan:
					if m.Content.Content == nil || len(m.Content.Content) == 0 {
						t.Errorf("wrong message data")
					}
					h.Insert(string(m.Content.Content))
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	initializeCluster := func(size int) ([]network.Transport, []*util.MessageHist) {
		var replicas []network.Transport
		var history []*util.MessageHist
		for i := 0; i < size; i++ {
			cfg := &types.PeerConfiguration{
				Name:          types.PeerName(fmt.Sprintf("%s-%d", partition, i)),
				Partition:     types.Partition(partition),
				Ctx:           ctx,
				ActionTimeout: time.Second,
			}
			if isCi {
				t.Logf("CI environment. Timeout is 20 seconds!")
				cfg.ActionTimeout = 20 * time.Second
			}
			trans, err := network.NewReliableTransport(cfg, definition.NewDefaultLogger())
			if err != nil {
				t.Fatalf("failed creating transport. %#v", err)
			}
			h := util.NewHistory()
			initializeReplica(trans, h)

			replicas = append(replicas, trans)
			history = append(history, h)
		}
		return replicas, history
	}

	listenersGroup.Add(clusterSize)
	replicas, history := initializeCluster(clusterSize)

	entry := replicas[0]
	writersGroup.Add(testSize)
	for i := 0; i < testSize; i++ {
		write := func(data []byte) {
			defer writersGroup.Done()
			err := entry.Broadcast(types.Message{
				Content: types.DataHolder{
					Content: data,
				},
				Destination: []types.Partition{types.Partition(partition)},
			})

			if err != nil {
				t.Errorf("failed broadcasting. %v", err)
			}
		}
		go write([]byte(fmt.Sprintf("%d", i)))
	}

	writersGroup.Wait()
	time.Sleep(10 * time.Second)
	cancel()
	listenersGroup.Wait()

	truth := history[0]
	if truth.Size() != testSize {
		t.Errorf("should have size %d, found %d", testSize, truth.Size())
	}

	for i, messageHist := range history {
		diff := truth.Compare(*messageHist)
		if diff != 0 {
			t.Errorf("history differ at %d with %d different commands", i, diff)
		}
	}

	for _, replica := range replicas {
		replica.Close()
	}
}
