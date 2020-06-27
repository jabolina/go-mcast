package mcast

import "github.com/jabolina/go-mcast/internal"

// Creates a new multicast instance for the partition with the
// given name. This will use the default configuration.
func NewMulticast(name internal.Partition) (internal.Unity, error) {
	return NewMulticastConfigured(DefaultConfiguration(name))
}

// Create a new multicast instance using the given configuration.
func NewMulticastConfigured(configuration *internal.Configuration) (internal.Unity, error) {
	return internal.NewUnity(configuration)
}

// Creates the default configuration for a partition with the given
// name. This will not use a stable storage nor a real conflict
// relationship for the messages.
func DefaultConfiguration(name internal.Partition) *internal.Configuration {
	return &internal.Configuration{
		Name:        name,
		Replication: 3,
		Version:     internal.LatestProtocolVersion,
		Conflict:    &AlwaysConflict{},
		Storage:     NewInMemoryStorage(),
		Logger:      NewDefaultLogger(),
	}
}
