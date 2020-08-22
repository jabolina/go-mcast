package test

import (
	"bytes"
	"fmt"
	"github.com/jabolina/go-mcast/internal"
	"github.com/jabolina/go-mcast/pkg/mcast"
	"runtime"
	"sync"
	"testing"
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
func NewInvoker() internal.Invoker {
	return &TestInvoker{
		group: &sync.WaitGroup{},
	}
}

type UnityCluster struct {
	T       *testing.T
	Names   []internal.Partition
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

func NewTestingUnity(configuration *internal.Configuration) (mcast.Unity, error) {
	invk := NewInvoker()
	var peers []internal.PartitionPeer
	for i := 0; i < configuration.Replication; i++ {
		pc := &internal.PeerConfiguration{
			Name:      fmt.Sprintf("%s-%d", configuration.Name, i),
			Partition: configuration.Name,
			Version:   configuration.Version,
			Conflict:  configuration.Conflict,
			Storage:   configuration.Storage,
		}
		peer, err := internal.NewPeer(pc, configuration.Logger)
		if err != nil {
			return nil, err
		}

		peers = append(peers, peer)
	}
	pu := &mcast.PeerUnity{
		Configuration: configuration,
		Peers:         peers,
		Last:          0,
		Invoker:       invk,
	}
	return pu, nil
}

func CreateUnity(name internal.Partition, t *testing.T) mcast.Unity {
	conf := mcast.DefaultConfiguration(name)
	conf.Logger.ToggleDebug(false)
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
		Names: make([]internal.Partition, clusterSize),
	}
	var unities []mcast.Unity
	for i := 0; i < clusterSize; i++ {
		name := internal.Partition(fmt.Sprintf("%s-%s", prefix, internal.GenerateUID()))
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

func (c UnityCluster) DoesClusterMatchTo(key []byte, expected []byte) {
	r := GenerateRandomRequestValue(key, c.Names)
	for i, unity := range c.Unities {
		res, err := unity.Read(r)
		if err != nil {
			c.T.Errorf("failed reading from partition 1. %v", err)
			continue
		}

		if !res.Success {
			c.T.Errorf("reading partition 1 failed. %v", res.Failure)
			continue
		}

		if !bytes.Equal(expected, res.Data) {
			c.T.Errorf("peer %d differ. %s|%s but expected %s", i, string(res.Data), res.Identifier, string(expected))
		}
	}
}

func (c UnityCluster) DoesAllClusterMatch(key []byte) {
	first := c.Unities[0]
	r := GenerateRandomRequestValue(key, c.Names)
	res, err := first.Read(r)
	if err != nil {
		c.T.Errorf("failed reding first peer. %v", err)
		return
	}

	if !res.Success {
		c.T.Errorf("something wrong readin. %v", res.Failure)
		return
	}

	c.T.Logf("cluster agrees on %s with %s", string(res.Data), res.Identifier)
	c.DoesClusterMatchTo(key, res.Data)
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
