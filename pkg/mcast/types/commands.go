package types

// Unique identifier to be associated with the message.
// When a request is made, the user will receive this unique
// identifier and the request will be processed throughout the
// whole protocol with this unique generated id.
type UID string

// A partition is a unique name given to a group of processes.
// This group of processes will work as a single unity and any
// external interaction will be as if the partition is a single
// process.
type Partition string

// Defines which kind of operation is the request doing.
type Operation string

// Describes at which state the current message is.
// Messages at state with value 0 do not have a timestamp yet.
// Message at state with value 1 received the local group timestamp.
// Message at state with value 2 received the final timestamp.
// Message at state with value 3 is ready to be delivered and the clock
// group is synchronized based on the message timestamp.
type MessageState uint8

// Simple uint8 for defining the kind of message is transported.
// Since this is not used by the protocol itself, will be used
// only on the header.
type MessageType uint8

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

	// The first type of message, to be processed locally
	// following the protocol definition and MessageState.
	Initial MessageType = iota

	// Defines a message a an external request, for example,
	// when exchanging the message timestamp between partitions.
	External

	// Defines the latest protocol version
	LatestProtocolVersion = 0

	// A command operation will change the a state on the
	// protocol state machine.
	Command Operation = "command"

	// A query operation will only read a value on the
	// protocol state machine.
	Query Operation = "query"
)

// Internal use only, to transport any specific
// information between the peers.
type ProtocolHeader struct {
	// Transport the configured version at which the protocol
	// will work, any increments in version must be described
	// and what have changed.
	ProtocolVersion uint

	// Information about the kind of message that will be
	// processed.
	Type MessageType
}

// Implemented by the protocol messages that will be sent
// internally by the protocol.
type HeaderExtract interface {
	Extract() ProtocolHeader
}

// The user will only use this when sending a request into
// the protocol.
// This is the most simple struct that can be created and
// the final user do not have to handle information about
// the protocol.
type Request struct {
	// The request key the value will be associated with.
	Key []byte

	// The concrete value that will be replicated.
	Value []byte

	// Any extra information that will be replicated along
	// with the value.
	Extra []byte

	// Partitions that will receive the request.
	Destination []Partition
}

// The final user will only receive as response what is
// important, e.g, the replicated data.
// No error message will be propagated back to the user,
// is this the right choice?
type Response struct {
	// The request was completed successfully.
	Success bool

	// After sending a message, the user will receive the
	// message unique identifier to verify if the value
	// is already committed on the state machine.
	Identifier UID

	// Replicated data.
	Data []byte

	// Replicated extra information.
	Extra []byte

	// If an error happened, this will transfer the
	// error back.
	Failure error
}

// Structure used internally by the protocol between peers.
type Message struct {
	// Header for specification management.
	Header ProtocolHeader

	// Message unique identifier.
	// When replicating a message, during all protocol
	// it must always hold the same identifier.
	Identifier UID

	// The information about the request.
	// Contains the information to be replicated and
	// which kind of request is this.
	Content DataHolder

	// Protocol message state for processing.
	State MessageState

	// Message timestamp.
	Timestamp uint64

	// Partitions that participate on the request.
	Destination []Partition

	// Partition who sent the message.
	From Partition
}

// Extract the message header.
func (m *Message) Extract() ProtocolHeader {
	return m.Header
}

// This method compares two messages for sorting reasons, following
// the already defined sorting for the protocol.
// First we verify the messages timestamps and if both are equal,
// then sort the message using the UID.
// For this method exists 3 results:
//
// m < m2 -> -1
// m > m2 -> 1
// m = m2 -> 0
//
// Even though exists the possibility for the value `0` be returned,
// this should not happen, since all messages will have unique identifiers.
func (m Message) Cmp(m2 Message) int {
	if m.Timestamp < m2.Timestamp {
		return -1
	}

	if m.Timestamp > m2.Timestamp {
		return 1
	}

	keyA := string(m.Identifier)
	keyB := string(m2.Identifier)
	if keyA < keyB {
		return -1
	}

	if keyA > keyB {
		return 1
	}
	return 0
}

// Verify if the two messages are different.
// To be different we must verify only the Identifier,
// Timestamp and State.
// It can be the same message, but with a updated Timestamp
// or State.
func (m Message) Diff(m2 Message) bool {
	return m.Identifier != m2.Identifier || m.Timestamp != m2.Timestamp || m.State != m2.State
}
