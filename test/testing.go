package test

import (
	"bytes"
	"fmt"
	"github.com/jabolina/go-mcast/internal"
	"github.com/jabolina/go-mcast/pkg/mcast"
	"sync"
	"testing"
)

type UnityCluster struct {
	T       *testing.T
	Names   []internal.Partition
	Unities []mcast.Unity
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

func CreateUnity(name internal.Partition, t *testing.T) mcast.Unity {
	conf := mcast.DefaultConfiguration(name)
	conf.Logger.ToggleDebug(true)
	unity, err := mcast.NewMulticastConfigured(conf)
	if err != nil {
		t.Fatalf("failed creating unity %s. %v", name, err)
	}
	return unity
}

func CreateCluster(clusterSize int, prefix string, t *testing.T) *UnityCluster {
	cluster := &UnityCluster{
		T:     t,
		group: &sync.WaitGroup{},
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
	defer func() {
		c.index += 1
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
			c.T.Errorf("peer %d differ. %s but expected %s", i, string(res.Data), string(expected))
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

	c.DoesClusterMatchTo(key, res.Data)
}

func (c *UnityCluster) PoweroffUnity(unity mcast.Unity) {
	defer c.group.Done()
	unity.Shutdown()
}
