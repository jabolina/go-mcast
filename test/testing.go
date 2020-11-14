package test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast"
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/prometheus/common/log"
	"runtime"
	"sync"
	"testing"
	"time"
)

type TestInvoker struct {
	group *sync.WaitGroup
}

func (t *TestInvoker) Spawn(f func()) {
	t.group.Add(1)
	go func() {
		defer t.group.Done()
		f()
	}()
}

func (t *TestInvoker) Stop() {
	t.group.Wait()
}
func NewInvoker() core.Invoker {
	return &TestInvoker{
		group: &sync.WaitGroup{},
	}
}

type UnityCluster struct {
	T       *testing.T
	Names   []types.Partition
	Unities []mcast.Unity
	mutex   *sync.Mutex
	group   *sync.WaitGroup
	index   int
}

func (c *UnityCluster) Off() {
	for _, unity := range c.Unities {
		c.group.Add(1)
		go c.PoweroffUnity(unity)
	}

	c.group.Wait()
}

func NewTestingUnity(configuration *types.Configuration) (mcast.Unity, error) {
	invk := NewInvoker()
	var peers []core.PartitionPeer
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < configuration.Replication; i++ {
		pc := &types.PeerConfiguration{
			Name:      fmt.Sprintf("%s-%d", configuration.Name, i),
			Partition: configuration.Name,
			Version:   configuration.Version,
			Conflict:  configuration.Conflict,
			Storage:   configuration.Storage,
			Ctx:       ctx,
			Cancel:    cancel,
		}
		peer, err := core.NewPeer(pc, configuration.Logger)
		if err != nil {
			cancel()
			for _, prevCreated := range peers {
				prevCreated.Stop()
			}
			return nil, err
		}

		peers = append(peers, peer)
	}
	pu := &mcast.PeerUnity{
		Configuration: configuration,
		Peers:         peers,
		Last:          0,
		Invoker:       invk,
		Finish:        cancel,
	}
	return pu, nil
}

func CreateUnity(name types.Partition, t *testing.T) mcast.Unity {
	conf := mcast.DefaultConfiguration(name)
	conf.Logger.ToggleDebug(false)
	conf.Logger.AddContext(string(name))
	unity, err := NewTestingUnity(conf)
	if err != nil {
		t.Fatalf("failed creating unity %s. %v", name, err)
	}
	return unity
}

func CreateCluster(clusterSize int, prefix string, t *testing.T) *UnityCluster {
	cluster := &UnityCluster{
		T:     t,
		group: &sync.WaitGroup{},
		mutex: &sync.Mutex{},
		Names: make([]types.Partition, clusterSize),
	}
	var unities []mcast.Unity
	for i := 0; i < clusterSize; i++ {
		name := types.Partition(fmt.Sprintf("%s-%s", prefix, helper.GenerateUID()))
		cluster.Names[i] = name
		unities = append(unities, CreateUnity(name, t))
	}
	cluster.Unities = unities
	return cluster
}

func (c *UnityCluster) Next() mcast.Unity {
	c.mutex.Lock()
	defer func() {
		c.index += 1
		c.mutex.Unlock()
	}()

	if c.index >= len(c.Unities) {
		c.index = 0
	}

	return c.Unities[c.index]
}

func (c UnityCluster) DoesClusterMatchTo(expected []types.DataHolder) {
	for _, unity := range c.Unities {
		res := unity.Read()

		if !res.Success {
			c.T.Errorf("reading partition 1 failed. %v", res.Failure)
			continue
		}
		outputValues(res.Data, string(unity.WhoAmI()))
		if len(res.Data) != len(expected) {
			c.T.Errorf("C-Hist differ on size, expected %d found %d", len(expected), len(res.Data))
		}

		for index, holder := range res.Data {
			expectedData := expected[index]
			if !bytes.Equal(expectedData.Content, holder.Content) {
				c.T.Errorf("Content differ cmd %d for unity %s, expected %#v, found %#v", index, unity.WhoAmI(), expectedData, holder)
				continue
			}
		}
	}
}

func outputValues(values []types.DataHolder, owner string) {
	log.Infof("--------------------%s-------------------------", owner)
	for _, value := range values {
		log.Infof("%s - %d\n", value.Meta.Identifier, value.Meta.Timestamp)
	}
}

func (c UnityCluster) DoesAllClusterMatch() {
	first := c.Unities[0]
	res := first.Read()

	if !res.Success {
		c.T.Errorf("something wrong readin. %v", res.Failure)
		return
	}
	c.DoesClusterMatchTo(res.Data)
}

func (c *UnityCluster) PoweroffUnity(unity mcast.Unity) {
	defer c.group.Done()
	unity.Shutdown()
}

func PrintStackTrace(t *testing.T) {
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	t.Errorf("%s", buf)
}

func WaitThisOrTimeout(cb func(), duration time.Duration) bool {
	done := make(chan bool)
	go func() {
		cb()
		done <- true
	}()
	select {
	case <-done:
		return true
	case <-time.After(duration):
		return false
	}
}
