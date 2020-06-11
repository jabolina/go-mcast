package amcast

import (
	"go-mcast/pkg/mcast"
)

// Bootstrap the group transports for the given configurations.
// This will create a transport for each peer present on the ClusterConfiguration.
func BootstrapGroup(base *mcast.BaseConfiguration, cluster *mcast.ClusterConfiguration) (*GroupState, error) {
	var nodes []NodeState
	for _, server := range cluster.Servers {
		config := &mcast.NetworkTransportConfig{
			ServerAddressResolver: cluster.TransportConfiguration.Resolver,
			Logger:                base.Logger,
			MaxPool:               int(cluster.TransportConfiguration.PoolSize),
			Timeout:               cluster.TransportConfiguration.Timeout,
		}
		trans, err := mcast.NewTCPTransportWithConfig(string(server.Address), cluster.TransportConfiguration.UseAdvertiseAddress, config)
		if err != nil {
			return nil, err
		}
		node := NodeState{
			Server: server,
			Trans:  trans,
		}
		nodes = append(nodes, node)
	}

	return &GroupState{
		Nodes: nodes,
		Clk:   LogicalClock{},
	}, nil
}
