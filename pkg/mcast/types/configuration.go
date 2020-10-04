package types

// Holds the peer configuration.
type PeerConfiguration struct {
	// The peer name.
	Name string

	// Which partition does this peer belongs to.
	Partition Partition

	// Version at which the peer is working.
	Version uint

	// Conflict relationship, will be used to order the
	// delivery sequence.
	Conflict ConflictRelationship

	// Stable storage to commit the values of the state
	// machine.
	Storage Storage
}

// The configuration for using the atomic multicast.
type Configuration struct {
	// The name for this multicast partition.
	// This will also be used to create an exchange on
	// the RabbitMQ broker. So, this name must be unique
	// across multiple partitions.
	Name Partition

	// The replication factor of how many peers must
	// this partition create.
	Replication int

	// Which version of the protocol will be used.
	Version uint

	// The conflict relationship that will be used
	// to order the requests for delivery.
	Conflict ConflictRelationship

	// Stable storage to maintaining the state machine data.
	Storage Storage

	// Logger to be used by the protocol.
	Logger Logger
}
