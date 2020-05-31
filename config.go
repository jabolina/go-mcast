package go_mcast

// The version of the protocol, that includes RPC messages
// and the implementation specific treatment. Use the
// ProtocolVersion member on the config to control the protocol
// version at which the servers will communicate with each other.
// This enables the possibility to easy backwards compatibility, and
// if unsure to which version to use, just jump to the most recent version.
// Each new version will have a short description of each version
//
// 0: Original and first available implementation.
type ProtocolVersion uint
