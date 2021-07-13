package types

// Unique identifier to be associated with the message.
// When a request is made, the user will receive this unique
// identifier and the request will be processed throughout the
// whole protocol with this unique generated id.
type UID string

// The name for a participant inside the protocol.
// This should be unique for each participant on the protocol,
// is not used in fact by the protocol, but used as a helper when
// debugging or resolving addresses.
type PeerName string

// A partition is a unique name given to a group of processes.
// This group of processes will work as a single unity and any
// external interaction will be as if the partition is a single
// process.
type Partition string

// Address the participant will bind to.
type TCPAddress string

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
	ABCast MessageType = iota

	// Defines a message a an external request, for example,
	// when exchanging the message timestamp between partitions.
	Network

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

	// The response data for the client.
	// This is a slice of DataHolder in case the requested
	// operation is to Dump the log file.
	// At this moment, the response will be the data available
	// on the Log, when a Command Query is executed the whole
	// sequence of commands will be returned.
	Data []DataHolder

	// If an error happened, this will transfer the error back
	// to the client.
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
func (m Message) HasHigherPriority(m2 Message) bool {
	if m.Timestamp < m2.Timestamp {
		return true
	}

	if m.Timestamp > m2.Timestamp {
		return false
	}

	if m.Identifier < m2.Identifier {
		return true
	}
	return false
}

// Verify if the two messages are different.
// To be different we must verify the Identifier, Timestamp and State.
// It can be a message with same Identifier, but with an Updated version,
// thus having a Timestamp and State greater than the actual one.
// Here too we are enforcing that we only forward on time.
func (m Message) Diff(m2 Message) bool {
	if m.Identifier == m2.Identifier {
		return m.Updated(m2)
	}
	return true
}

// Verify if the given message is an updated version.
// This is required so we can only go forward on time
// and can handle any delayed message.
func (m Message) Updated(m2 Message) bool {
	return m.Identifier == m2.Identifier && m2.State > m.State
}
