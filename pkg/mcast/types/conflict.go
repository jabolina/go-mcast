package types

// Previous set used by the protocol for handling
// conflicts and ordering messages.
// This set *must* be thread safety.
type ConflictRelationship interface {
	// Verify if the given message conflicts with
	// previous messages on the set.
	Conflict(message Message, messages []Message) bool
}
