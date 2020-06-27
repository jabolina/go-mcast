package mcast

import "github.com/jabolina/go-mcast/internal"

// Creates a new multicast instance for the partition with the
// given name. This will use the default configuration.
func NewMulticast(name internal.Partition) (Unity, error) {
	return NewMulticastConfigured(DefaultConfiguration(name))
}

// Create a new multicast instance using the given configuration.
func NewMulticastConfigured(configuration *internal.Configuration) (Unity, error) {
	return NewUnity(configuration)
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

// Creates a new partition name for the given string value.
func CreatePartitionName(name string) internal.Partition {
	return internal.Partition(name)
}

// Create a new write request, to write value given value and extra
// associated with the given key, the request will be sent
// to the given destinations.
func NewWriteRequest(key, value, extra []byte, destination []internal.Partition) *internal.Request {
	return &internal.Request{
		Key:         key,
		Value:       value,
		Extra:       extra,
		Destination: destination,
	}
}

// Creates a read request, to read the given key for one of the
// given destinations.
func NewReadRequest(key []byte, destination []internal.Partition) *internal.Request {
	return &internal.Request{
		Key: key,
		Destination: destination,
	}
}
