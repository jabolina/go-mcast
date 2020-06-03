package internal

import "go-mcast/pkg/mcast"

func BootstrapCluster(configuration *mcast.Config, clusterConfiguration mcast.ClusterConfiguration, trans mcast.Transport) error {
	if err := mcast.ValidateConfig(configuration); err != nil {
		return err
	}

	if err := mcast.ValidateClusterConfiguration(clusterConfiguration); err != nil {
		return err
	}
	return nil
}
