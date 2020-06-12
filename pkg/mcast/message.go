package mcast

// Describes at which state the current message is.
// Messages at state with value 0 do not have a timestamp yet.
// Message at state with value 1 received the local group timestamp.
// Message at state with value 2 received the final timestamp.
// Message at state with value 3 is ready to be delivered and the clock
// group is synchronized based on the message timestamp.
type MessageState uint8

// When a request is made, it will be of different types,
// or a Command or Query. Command is when the entry is touched,
// this can be entries that are created, updated or deleted.
// Query only wants to read the entry value.
type Operation uint8

const (
	// Message do not have a timestamp yet.
	S0 MessageState = iota

	// Message received the local group timestamp.
	S1

	// Message received the final timestamp.
	S2

	// Message is ready to be delivered and the clock
	// group is synchronized based on the message timestamp.
	S3

	// Command operations changes the state machine, this
	// operation can add, update or delete an entry into
	// the state machine.
	Command Operation = iota

	// Query operations can only be used when reading a
	// value from the state machine.
	Query
)

type DataHolder struct {
	// What kind of operation is being executed.
	Operation

	// This will be used to associate the value with something
	// so the retrieval can be done more easily.
	// If nothing is provided, will be generated a value to
	// be used based on the cluster information.
	Key string

	// If there is any value to be written into the
	// state machine it will be hold here.
	// This will only have a value if the operation is
	// of type command.
	// If the value is nil, then the state machine value
	// associated will also be nil.
	Content []byte
}

// Message to be replicated across all members of the cluster
type Message struct {
	// Holds information about the message state.
	MessageState MessageState

	// Holds the value for the message timestamp.
	Timestamp uint64

	// Holds the information to be replicated across replicas.
	// This a marshalled version of the DataHolder struct.
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate.
	Extensions []byte
}

// Entry that is committed into the state machine.
type Entry struct {
	// Holds the final timestamp when the message was committed.
	FinalTimestamp uint64

	// Holds the information to be replicated across replicas.
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate.
	// The entry will hold the same extension sent by the used.
	Extensions []byte
}
