package types

// ConflictRelationship used by the protocol for handling conflicts and
// ordering messages.
type ConflictRelationship interface {
	// Conflict verify if the given message conflicts with
	// previous messages on the set.
	Conflict(current Message, saved Message) bool
}
