package test

import (
	"go-mcast/pkg/mcast"
)

func InMemoryConfig() *mcast.BaseConfiguration {
	return &mcast.BaseConfiguration{
		Version: mcast.LatestProtocolVersion,
		Logger:  mcast.NewDefaultLogger(),
	}
}

func ClusterConfiguration(servers []mcast.Server) *mcast.ClusterConfiguration {
	return &mcast.ClusterConfiguration{
		Servers: servers,
	}
}
