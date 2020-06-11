package test

import (
	"go-mcast/pkg/mcast"
)

const BasePort = 8080

type TestAddressResolver struct {
}

func (t *TestAddressResolver) Resolve(id mcast.ServerID) (mcast.ServerAddress, error) {
	return mcast.ServerAddress(id), nil
}

func InMemoryConfig() *mcast.BaseConfiguration {
	return &mcast.BaseConfiguration{
		Version: mcast.LatestProtocolVersion,
		Logger:  mcast.NewDefaultLogger(),
	}
}

func ClusterConfiguration(servers []mcast.Server, transportConf mcast.TransportConfiguration) *mcast.ClusterConfiguration {
	return &mcast.ClusterConfiguration{
		Servers:                servers,
		TransportConfiguration: transportConf,
	}

}
