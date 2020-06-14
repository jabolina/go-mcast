package test

import (
	"fmt"
	"go-mcast/pkg/mcast"
	"sync"
	"testing"
)

type PortProvider struct {
	base  int
	mutex *sync.Mutex
}

func (p *PortProvider) GetAndAdd() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	value := p.base
	p.base += 1
	return value
}

func (p *PortProvider) Remove(size int) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.base -= size
}

var DefaultPortResolver = PortProvider{
	base:  8080,
	mutex: &sync.Mutex{},
}

type TestAddressResolver struct {
}

func (t *TestAddressResolver) Resolve(id mcast.ServerID) (mcast.ServerAddress, error) {
	return mcast.ServerAddress(id), nil
}

type UnityCluster struct {
	T       *testing.T
	Unities []*mcast.Unity
	group   *sync.WaitGroup
}

func (u *UnityCluster) Peers() []*mcast.Peer {
	var peers []*mcast.Peer
	for _, unity := range u.Unities {
		peers = append(peers, unity.ResolvePeer())
	}
	return peers
}

func (c *UnityCluster) Off() {
	for _, unity := range c.Unities {
		c.group.Add(1)
		go c.PoweroffUnity(unity)
	}

	c.group.Wait()
}

func ClusterConfiguration(servers []mcast.Server, transportConf mcast.TransportConfiguration) *mcast.ClusterConfiguration {
	return &mcast.ClusterConfiguration{
		Servers:                servers,
		TransportConfiguration: transportConf,
	}
}

func CreateUnity(size int, t *testing.T) *mcast.Unity {
	base := mcast.DefaultBaseConfiguration()
	base.Logger.ToggleDebug(true)
	transportConfiguration := mcast.DefaultTransportConfiguration(&TestAddressResolver{})

	var servers []mcast.Server
	for i := 0; i < size; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", DefaultPortResolver.GetAndAdd())
		server := mcast.Server{
			ID:      mcast.ServerID(addr),
			Address: mcast.ServerAddress(addr),
		}
		servers = append(servers, server)
	}

	clusterConfiguration := ClusterConfiguration(servers, *transportConfiguration)
	storage := mcast.NewInMemoryStorage()
	clock := &mcast.LogicalClock{}

	unity, err := mcast.NewAtomicMulticast(base, clusterConfiguration, storage, clock)
	if err != nil {
		t.Fatalf("failed creating unity %v", err)
	}
	return unity
}

func CreateCluster(clusterSize, unitySize int, t *testing.T) *UnityCluster {
	cluster := &UnityCluster{
		T:     t,
		group: &sync.WaitGroup{},
	}
	var unities []*mcast.Unity
	for i := 0; i < clusterSize; i++ {
		unities = append(unities, CreateUnity(unitySize, t))
	}
	cluster.Unities = unities
	return cluster
}

func (c *UnityCluster) PoweroffUnity(unity *mcast.Unity) {
	defer c.group.Done()
	unity.Shutdown()
	DefaultPortResolver.Remove(len(unity.State.Nodes))
}
