package mcast

import "sync"

// Bootstrap the group transports for the given configurations.
// This will create a transport for each peer present on the ClusterConfiguration.
func BootstrapGroup(base *BaseConfiguration, cluster *ClusterConfiguration, unity *Unity) (*GroupState, error) {
	state := &GroupState{
		Clk:   LogicalClock{},
		group: &sync.WaitGroup{},
		mutex: &sync.Mutex{},
	}
	var nodes []*Peer
	for _, server := range cluster.Servers {
		config := &NetworkTransportConfig{
			ServerAddressResolver: cluster.TransportConfiguration.Resolver,
			Logger:                base.Logger,
			MaxPool:               int(cluster.TransportConfiguration.PoolSize),
			Timeout:               cluster.TransportConfiguration.Timeout,
		}
		trans, err := NewTCPTransportWithConfig(string(server.Address), cluster.TransportConfiguration.UseAdvertiseAddress, config)
		if err != nil {
			return nil, err
		}

		nodes = append(nodes, NewPeer(server, trans, state, unity))
	}

	state.Nodes = nodes
	return state, nil
}
