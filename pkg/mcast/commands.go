package mcast

import (
	"go-mcast/internal/remote"
)

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

// GMCastRequest is the command used to replicate a value
// across replicas
type GMCastRequest struct {
	RPCHeader

	// Provides the request unique identifier across all steps
	UID UID

	// The message payload to be replicated
	Body remote.Message

	// Addresses to send the request
	Destination []ServerAddress
}

// Response for the GMCastRequest
type GMCastResponse struct {
	RPCHeader

	// The request final timestamp, agreed amongst all replicas
	SequenceNumber uint64

	// The request completed successfully
	Success bool
}

// Used the ask peers when computing the group timestamp.
type ComputeRequest struct {
	RPCHeader

	UID

	// State needed for the message computation.
	State remote.MessageState

	// Request timestamp
	Timestamp uint64

	// Addresses to send the request
	Destination []ServerAddress
}

// After a peer computes its timestamp this will be used to answer.
type ComputeResponse struct {
	RPCHeader

	UID

	// Response timestamp for the single peer, later this will be
	// agreed upon a single value amongst all peers.
	Timestamp uint64

	// New state for the processed request.
	State remote.MessageState
}

// When a message is destined to more than one destination group,
// the timestamp must be exchanged between all groups and a final
// timestamp value will be agreed.
type GatherRequest struct {
	RPCHeader

	UID

	// State used to process the gather request.
	State remote.MessageState

	// Timestamp for the group, this will be exchanged with other remote servers.
	Timestamp uint64
}

// Response after the timestamp was exchanged, contains the final
// timestamp that was agreed amongst all groups.
type GatherResponse struct {
	RPCHeader

	UID

	// State after the message was processed.
	State remote.MessageState

	// The final timestamp agreed between all groups. This will
	// be the final timestamp and now all groups will be synchronized.
	Timestamp uint64
}

// Get the GM-Cast response RPC header information
func (res *GMCastResponse) GetRPCHeader() RPCHeader {
	return res.RPCHeader
}
