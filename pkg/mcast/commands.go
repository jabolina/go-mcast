package mcast

// RPCHeader is common sub-structure between request to pass common
// needed information about functionalities.
type RPCHeader struct {
	// Protocol version at which servers must communicate
	// Latest: 0
	ProtocolVersion ProtocolVersion
}

// Unique identifier
type UID string

// Exposes the RPC header
type WithRPCHeader interface {
	GetRPCHeader() RPCHeader
}

// The user will only use this when sending a request into
// the protocol.
// This is the most simple struct that can be created and
// the final user do not have to handle information about
// the protocol.
type Request struct {
	RPCHeader

	// Which kind of operation is the request.
	Operation

	// The request key the value will be associated with.
	Key []byte

	// The concrete value that will be replicated.
	Value []byte

	// Any extra information that will be replicated along
	// with the value.
	Extra []byte

	// Addresses to send the request
	Destination []Server
}

// The final user will only receive as response what is
// important, e.g, the replicated data.
// No error message will be propagated back to the user,
// is this the right choice?
type Response struct {
	// The request was completed successfully.
	Success bool

	// The current value present at the state machine.
	Value []byte

	// Extra information also replicated with the data.
	Extra []byte
}

// GMCastRequest is the command used to replicate a value
// across replicas
type GMCastRequest struct {
	RPCHeader

	// Provides the request unique identifier across all steps
	UID UID

	// The message payload to be replicated
	Body Message

	// Addresses to send the request
	Destination []Server
}

// Response for the GMCastRequest
type GMCastResponse struct {
	RPCHeader

	// The request final timestamp, agreed amongst all replicas.
	SequenceNumber uint64

	// The content on the state machine.
	Body Message

	// The request completed successfully.
	Success bool
}

// Used the ask peers when computing the group timestamp.
type ComputeRequest struct {
	RPCHeader

	UID

	// State needed for the message computation.
	State MessageState

	// Request timestamp.
	Timestamp uint64

	// Addresses to send the request.
	Destination []Server
}

// After a peer computes its timestamp this will be used to answer.
type ComputeResponse struct {
	RPCHeader

	UID

	// Response timestamp for the single peer, later this will be
	// agreed upon a single value amongst all peers.
	Timestamp uint64

	// New state for the processed request.
	State MessageState
}

// When a message is destined to more than one destination group,
// the timestamp must be exchanged between all groups and a final
// timestamp value will be agreed.
type GatherRequest struct {
	RPCHeader

	UID

	// State used to process the gather request.
	State MessageState

	// Timestamp for the group, this will be exchanged with other remote servers.
	Timestamp uint64

	// Addresses to send the request.
	Destination []Server
}

// Response after the timestamp was exchanged, contains the final
// timestamp that was agreed amongst all groups.
type GatherResponse struct {
	RPCHeader

	UID

	// State after the message was processed.
	State MessageState

	// The final timestamp agreed between all groups. This will
	// be the final timestamp and now all groups will be synchronized.
	Timestamp uint64
}

// Request defines protocol version header.
func (r *Request) GetRPCHeader() RPCHeader {
	return r.RPCHeader
}

func (req *GMCastRequest) GetRPCHeader() RPCHeader {
	return req.RPCHeader
}

// Get the GM-Cast response RPC header information
func (res *GMCastResponse) GetRPCHeader() RPCHeader {
	return res.RPCHeader
}

func (c *ComputeRequest) GetRPCHeader() RPCHeader {
	return c.RPCHeader
}

func (g *GatherRequest) GetRPCHeader() RPCHeader {
	return g.RPCHeader
}
