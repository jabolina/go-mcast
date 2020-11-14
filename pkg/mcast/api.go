package mcast

import (
	"github.com/jabolina/go-mcast/pkg/mcast/definition"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// Creates a new multicast instance for the partition with the
// given name. This will use the default configuration.
func NewMulticast(name types.Partition) (Unity, error) {
	return NewMulticastConfigured(DefaultConfiguration(name))
}

// Create a new multicast instance using the given configuration.
func NewMulticastConfigured(configuration *types.Configuration) (Unity, error) {
	return NewUnity(configuration)
}

// Creates the default configuration for a partition with the given
// name. This will not use a stable storage nor a real conflict
// relationship for the messages.
func DefaultConfiguration(name types.Partition) *types.Configuration {
	return &types.Configuration{
		Name:        name,
		Replication: 3,
		Version:     types.LatestProtocolVersion,
		Conflict:    &definition.AlwaysConflict{},
		Storage:     definition.NewDefaultStorage(),
		Logger:      definition.NewDefaultLogger(),
	}
}

// Creates a new partition name for the given string value.
func CreatePartitionName(name string) types.Partition {
	return types.Partition(name)
}

// Create a new write request, to write value given value and extra
// associated with the given key, the request will be sent
// to the given destinations.
func NewWriteRequest(key, value, extra []byte, destination []string) *types.Request {
	var partitions []types.Partition
	for _, s := range destination {
		partitions = append(partitions, types.Partition(s))
	}
	return &types.Request{
		Value:       value,
		Extra:       extra,
		Destination: partitions,
	}
}

// Creates a read request, to read the given key for one of the
// given destinations.
func NewReadRequest(key []byte, destination []string) *types.Request {
	var partitions []types.Partition
	for _, s := range destination {
		partitions = append(partitions, types.Partition(s))
	}
	return &types.Request{
		Destination: partitions,
	}
}
