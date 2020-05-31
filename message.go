package go_mcast

// Describes at which state the current message is.
// Messages at state with value 0 do not have a timestamp yet.
// Message at state with value 1 received the local group timestamp.
// Message at state with value 2 received the final timestamp.
// Message at state with value 3 is ready to be delivered and the clock
// group is synchronized based on the message timestamp.
type State uint8

const (
	// Message do not have a timestamp yet.
	S0 State = iota

	// Message received the local group timestamp.
	S1

	// Message received the final timestamp.
	S2

	// Message is ready to be delivered and the clock
	// group is synchronized based on the message timestamp.
	S3
)

// Message to be replicated across all members of the cluster
type Message struct {
	// Holds information about the message state.
	State State

	// Holds the value for the message timestamp.
	Timestamp uint64

	// Holds the information to be replicated across replicas.
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate.
	Extensions []byte
}
