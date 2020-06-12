package test

import (
	"fmt"
	"go-mcast/pkg/mcast"
	"testing"
)

var BasePort = 8080

type TestAddressResolver struct {
}

func (t *TestAddressResolver) Resolve(id mcast.ServerID) (mcast.ServerAddress, error) {
	return mcast.ServerAddress(id), nil
}

type UnityCluster struct {
	T       *testing.T
	Unities []*mcast.Unity
}

func (c *UnityCluster) Off() {
	for _, unity := range c.Unities {
		PoweroffUnity(unity, c.T)
	}
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
		addr := fmt.Sprintf("127.0.0.1:%d", BasePort+i)
		server := mcast.Server{
			ID:      mcast.ServerID(addr),
			Address: mcast.ServerAddress(addr),
		}
		servers = append(servers, server)
	}
	BasePort += size

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
		T: t,
	}
	var unities []*mcast.Unity
	for i := 0; i < clusterSize; i++ {
		unities = append(unities, CreateUnity(unitySize, t))
	}
	cluster.Unities = unities
	return cluster
}

func PoweroffUnity(unity *mcast.Unity, t *testing.T) {
	t.Log("shutdown unity")
	future := unity.Shutdown()

	if err := future.Error(); err != nil {
		t.Fatalf("failed on shutdown %v", err)
	}

	BasePort -= len(unity.State.Nodes)
}
