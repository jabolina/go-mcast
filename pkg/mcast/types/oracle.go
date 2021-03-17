package types

// The oracle can resolve the addresses of participants.
// The oracle can tell the TCP address for any participant in the protocol,
// can resolve a single address by name, can resolve all addresses from a
// whole partition.
type Oracle interface {
	// The oracle resolves the address for the given peer name.
	ResolveByName(name PeerName) *TCPAddress

	// Resolve the address from all peers inside a partition.
	ResolveByPartition(partition Partition) []TCPAddress
}
