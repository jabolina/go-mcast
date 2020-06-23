package test

import (
	"fmt"
	"go-mcast/internal"
	"go-mcast/pkg/mcast"
	"sync"
	"testing"
)

type UnityCluster struct {
	T       *testing.T
	Unities []internal.Unity
	group   *sync.WaitGroup
}

func (c *UnityCluster) Off() {
	for _, unity := range c.Unities {
		c.group.Add(1)
		go c.PoweroffUnity(unity)
	}

	c.group.Wait()
}

func CreateUnity(name internal.Partition, t *testing.T) internal.Unity {
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
	}
	var unities []internal.Unity
	for i := 0; i < clusterSize; i++ {
		unities = append(unities, CreateUnity(internal.Partition(fmt.Sprintf("%s-%s", prefix, internal.GenerateUID())), t))
	}
	cluster.Unities = unities
	return cluster
}

func (c *UnityCluster) PoweroffUnity(unity internal.Unity) {
	defer c.group.Done()
	unity.Shutdown()
}
