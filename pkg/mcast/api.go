package mcast

func NewAtomicMulticast(base *BaseConfiguration, cluster *ClusterConfiguration, storage Storage, clock LogicalGlobalClock) (*Unity, error) {
	if err := ValidateBaseConfiguration(base); err != nil {
		return nil, err
	}

	if err := ValidateClusterConfiguration(cluster); err != nil {
		return nil, err
	}

	if err := ValidateTransportConfiguration(&cluster.TransportConfiguration); err != nil {
		return nil, err
	}

	if base.Logger == nil {
		base.Logger = NewDefaultLogger()
	}

	unity, err := NewUnity(base, cluster, storage, clock)
	if err != nil {
		return nil, err
	}

	return unity, nil
}
