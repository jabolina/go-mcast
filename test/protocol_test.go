package test

import (
	"fmt"
	"go-mcast/pkg/mcast"
	"testing"
)

func TestProtocol_BootstrapUnity(t *testing.T) {
	clusterSize := 5
	base := mcast.DefaultBaseConfiguration()
	transportConfiguration := mcast.DefaultTransportConfiguration(&TestAddressResolver{})

	var servers []mcast.Server
	for i := 0; i < clusterSize; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", BasePort+i)
		server := mcast.Server{
			ID:      mcast.ServerID(addr),
			Address: mcast.ServerAddress(addr),
		}
		servers = append(servers, server)
	}

	clusterConfiguration := ClusterConfiguration(servers, *transportConfiguration)
	storage := mcast.NewInMemoryStorage()
	clock := &mcast.LogicalClock{}

	_, err := mcast.NewAtomicMulticast(base, clusterConfiguration, storage, clock)
	if err != nil {
		t.Fatalf("failed creating unity. %v", err)
	}

}
