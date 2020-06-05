package mcast

import "go-mcast/internal"

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
	Body internal.Message

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

// Get the GM-Cast response RPC header information
func (res *GMCastResponse) GetRPCHeader() RPCHeader {
	return res.RPCHeader
}
